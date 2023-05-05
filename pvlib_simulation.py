from datetime import date, datetime, timedelta

import pandas as pd
import pvlib

from db_connector import DBConnector

DICT_MOUNT_CLASS = {
    -3.47: "open_rack_glass_glass",
    -2.98: "close_mount_glass_glass",
    -3.56: "open_rack_glass_polymer",
    -2.81: "insulated_back_glass_polymer",
}


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def _get_simulation(df_config, losses_model="pvwatts", losses_parameters=None):
    """
    Simulate energy using PVLIB
    """
    lat = df_config["lat"][0]
    lon = df_config["lon"][0]
    panel_tilt = df_config["tilt"][0]
    panel_azimuth = df_config["azimuth"][0]
    module_parameters = {
        "pdc0": df_config["dc_power"][0],
        "gamma_pdc": df_config["temp_coeff"][0],
    }
    inverter_parameters = {
        "pdc0": df_config["dc_power"][0],
        "pac0": df_config["ac_power"][0],
    }
    temperature_model_parameters = pvlib.temperature.TEMPERATURE_MODEL_PARAMETERS[
        "sapm"
    ][DICT_MOUNT_CLASS[float(str(df_config["heat_transfer_constant_a"][0]))]]
    location = pvlib.location.Location(latitude=lat, longitude=lon)

    _PVWATTS_DEFAULT_LOSS_PARAM_ = {
        "soiling": 2,
        "shading": 3,
        "snow": 0,
        "mismatch": 2,
        "wiring": 2,
        "connections": 0.5,
        "lid": 1.5,
        "nameplate_rating": 1,
        "age": df_config["plant_age"][0],
        "availability": 3,
    }

    if losses_model == "pvwatts":

        if losses_parameters == None:
            losses_parameters = _PVWATTS_DEFAULT_LOSS_PARAM_
        else:
            def_lossparam_keys = list(_PVWATTS_DEFAULT_LOSS_PARAM_.keys())
            loss_param_keys = list(losses_parameters.keys())

            if len(list(set(loss_param_keys) - set(def_lossparam_keys))) == 0:
                for _ in list(set(def_lossparam_keys) - set(loss_param_keys)):
                    losses_parameters["key"] = _PVWATTS_DEFAULT_LOSS_PARAM_["key"]
            else:
                print("Error in function: _get_simulation")

    pvwatts_system = pvlib.pvsystem.PVSystem(
        surface_tilt=panel_tilt,
        surface_azimuth=panel_azimuth,
        module_parameters=module_parameters,
        inverter_parameters=inverter_parameters,
        temperature_model_parameters=temperature_model_parameters,
        losses_parameters=losses_parameters,
    )

    mc = pvlib.modelchain.ModelChain(
        pvwatts_system,
        location,
        aoi_model="no_loss",
        spectral_model="no_loss",
        losses_model=losses_model,
    )
    return mc


def get_simulation_per_inverter(id_p, date_to_query, lst_id_i):
    df_connector = DBConnector()
    query = f"""
        with base as (
            SELECT p.id ,
            ('{date_to_query}'::date + generate_series('{date_to_query}'::date, '{date_to_query}'::date + '1 day - 1 second'::interval,
                '5 min'::interval)::time)::text t
            FROM metadata.plant p
            where p.id = {id_p}
        )
        ,
        meteo as (
            select *
            from (
                select
                (date_trunc('hour', utc_localtime)
                    + date_part('minute', utc_localtime)::int / 5 * interval '5 min')::text t,
                sum(ghi) ghi,
                sum(dni) dni,
                sum(dhi) dhi,
                avg(wind_speed) wind_speed,
                avg(temperature) temp_air
                from (
                    select msd.*,
                    msd.t at time zone 'utc' at time zone 'America/Santiago' at time zone 'America/Santiago' as utc_localtime,
                    ms.periodicity_in_sec  periodicity_in_sec
                    from metadata.plant p
                    inner join metadata.meteo_station ms on p.id = ms.plant_id
                    inner join measurement.meteo_station_data msd on msd.meteo_station_id = ms.id
                    where p.id = {id_p}
                    and
                    date_trunc('day', msd.t at time zone 'utc' at time zone 'America/Santiago' at time zone 'America/Santiago') = '{date_to_query}'
                    order by msd.t asc
                )x
                group by 1, periodicity_in_sec
            ) x
            where wind_speed is not null  and temp_air is not null
        )
        select
        t,
        ghi,
        dni,
        dhi,
        coalesce(wind_speed, wind_speed_avg) wind_speed,
        coalesce(temp_air, temp_air_avg) temp_air
        from (
            select
            b.t,
            coalesce(m.ghi,0) ghi,
            coalesce(m.dni,0) dni,
            coalesce(m.dhi,0) dhi,
            m.wind_speed,
            m.temp_air,
            avg(m.wind_speed) over (order by b.t asc ) wind_speed_avg,
            avg(m.temp_air) over (order by b.t asc ) temp_air_avg
            from base b
            left join meteo m on b.t = m.t
        )x
        ;
    """
    df_pre_poa = df_connector.get_data_from_db(query)
    fechacol = "t"
    df_pre_poa = df_pre_poa.set_index(fechacol, drop=True)
    df_pre_poa.index = pd.to_datetime(df_pre_poa.index)
    df_result_concatenated = pd.DataFrame()
    for id_i in lst_id_i:
        query = f"""
            select
            p.lat_lon[0] lat,
            p.lat_lon[1] lon,
            p.soiling_rate,
            'America/Santiago' timezone,
            (EXTRACT(epoch FROM ((now()-p.t_op_start)/365))/3600)/24 plant_age,
            i.id,
            i.plant_id,
            i.temp_coeff/100*-1 temp_coeff,
            i.tilt,
            i.azimuth,
            i.heat_transfer_constant_a,
            i.heat_transfer_constant_b,
            i.heat_transfer_constant_delta,
            i.dc_power,
            i.ac_power
            from metadata.plant p
            inner join metadata.inverter i on p.id = i.plant_id
            where i.id = {id_i}
            ;
        """
        df_config = df_connector.get_data_from_db(query, cast_float=True)
        mc = _get_simulation(df_config)
        tz = str(df_config["timezone"][0])
        if id_i == lst_id_i[0]:
            df_pre_poa.index = df_pre_poa.index.tz_localize(
                tz, ambiguous=True, nonexistent="shift_forward"
            ).tz_convert(tz)
        mc.run_model(df_pre_poa)
        df_simulated = pd.DataFrame(
            {"t": mc.results.ac.index, "ac": mc.results.ac.values}
        )
        df_simulated["t"] = pd.to_datetime(df_simulated["t"])
        df_simulated["id"] = id_i
        df_result_concatenated = pd.concat([df_result_concatenated, df_simulated])

    return df_result_concatenated
