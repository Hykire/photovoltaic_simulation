import pandas as pd

from db_connector import DBConnector


class AutomateReport:
    def __init__(self, id_p, t_start, t_end) -> None:
        self.id_p = id_p
        self.t_start = t_start
        self.t_end = t_end
        self.db_connector = DBConnector()

    def get_active_power_from_db(self) -> pd.DataFrame:
        """Get active power of a period of time, by inverter.

        Returns:
            pd.DataFrame: return active power by inverter, 5 min
        """
        query_to_extract = f"""
        select
            i2.t, i.id ,
            case
                when i2.active_power < 0 then 0
                else i2.active_power
            end active_power
        from metadata.plant p
        inner join metadata.inverter i on p.id = i.plant_id
        inner join measurement.inverter i2 on i2.inverter_id = i.id
        where
        i2.t >= to_date('{self.t_start}', 'YYYY-MM-DD')
        and
        i2.t < to_date('{self.t_end}', 'YYYY-MM-DD')
        and p.id = {self.id_p}
        """
        df = self.db_connector.get_data_from_db(query_to_extract, False)
        return df

    def get_meteo_satellite_from_db(self):
        query_to_extract = f"""
        select
        msd.t at time zone 'utc' at time zone 'America/Santiago' at time zone 'America/Santiago' as t,
        msd.ghi
        from metadata.plant p
        inner join metadata.meteo_station ms on p.id = ms.plant_id
        inner join measurement.meteo_station_data msd on msd.meteo_station_id = ms.id
        where p.id = {self.id_p}
        and
        msd.t at time zone 'utc' at time zone 'America/Santiago' at time zone 'America/Santiago' >= '{self.t_start}'
        and
        msd.t at time zone 'utc' at time zone 'America/Santiago' at time zone 'America/Santiago' <= '{self.t_end}'
        order by msd.t asc
        ;
        """
        df = self.db_connector.get_data_from_db(query_to_extract, False)
        return df

    def get_meteo_pyra_from_db(self):
        query_to_extract = f"""
        select pm.*
        from public.piranometro p
        inner join public.piranometro_medicion pm on p.id_pi = pm.id_pi
        inner join public.inversor_piranometro ip on ip.id_piranometro = p.id_pi
        inner join public.inversor i on i.id_i = ip.id_inversor
        where i.id_p = {self.id_p}
        and
        fecha_pim >='{self.t_start}'
        and
        fecha_pim <='{self.t_end}'
        order by pm.fecha_pim asc
        ;
        """
        df = self.db_connector.get_data_from_db(query_to_extract, False)
        return df

    def get_plant_metadata_till_inverter(self):
        """
        Get plant's configuration for first part of the report
        """
        query_to_extract = f"""
        select ubicacion_p, potencia_ac_i , potencia_dc_i,
        i.marca_i , i.modelo_i , count(*) n_inverters
        from public.planta p
        inner join public.inversor i on p.id_p = i.id_p
        where p.id_p = {self.id_p}
        group by 1,2,3,4,5
        ;
        """
        df = self.db_connector.get_data_from_db(query_to_extract)
        return df

    def get_plant_metadata_strings(self):
        """
        Get plant's configuration for first part of the report
        """
        query_to_extract = f"""
        select p.id_p , si.tecnologia_string_inversor , si.panel_tilt_string_inversor , si.azimuth_string_inversor , count(*)
        from public.planta p
        inner join public.inversor i on p.id_p = i.id_p
        inner join public.string_inversor si on i.id_i = si.id_i
        where p.id_p = {self.id_p}
        group by 1,2,3,4
        ;
        """
        df = self.db_connector.get_data_from_db(query_to_extract)
        return df

    def get_plant_metadata_inverter_p2(self):
        """
        Get plant's configuration for first part of the report
        """
        query_to_extract = f"""
        select p.id_p , si.tecnologia_string_inversor , si.panel_tilt_string_inversor , si.azimuth_string_inversor , count(*)
        from public.planta p
        inner join public.inversor i on p.id_p = i.id_p
        inner join public.string_inversor si on i.id_i = si.id_i
        where p.id_p = {self.id_p}
        group by 1,2,3,4
        ;
        """
        df = self.db_connector.get_data_from_db(query_to_extract)
        return df

    def get_plant_metadata_strings_per_inverter_p2(self):
        """
        Get plant's configuration for first part of the report
        """
        query_to_extract = f"""
        select i.serial_number_i , i.potencia_ac_i , i.potencia_dc_i , count(*) n_string_per_inverter
        from public.planta p
        inner join public.inversor i on p.id_p = i.id_p
        inner join public.string_inversor si on i.id_i = si.id_i
        where p.id_p = {self.id_p}
        group by 1,2,3
        ;
        """
        df = self.db_connector.get_data_from_db(query_to_extract)
        return df

    def get_daily_kpis(self):
        """
        Get plant's configuration for first part of the report
        """
        query_to_extract = f"""
        select dd.fecha_dd , i.id_p, avg(performance_ratio_dd) pr, avg(availability_dd) availability, sum(energy_dd) energy
        from public.data_daily dd
        inner join public.inversor i on dd.id_i = i.id_i
        where dd.id_p = {self.id_p}
        and dd.energy_dd is not null and dd.energy_dd <> 0
        and dd.fecha_dd >= to_date('{self.t_start}', 'YYYY-MM-DD') AND dd.fecha_dd < to_date('{self.t_end}', 'YYYY-MM-DD')
        group by 1,2
        order by 1
        ;
        """
        df = self.db_connector.get_data_from_db(query_to_extract)
        return df

    def get_energy_dd_per_inverter(self):
        query_to_extract = f"""
        select dd.fecha_dd , i.serial_number_i, avg(performance_ratio_dd) pr, avg(availability_dd) availability, sum(energy_dd) energy
        from public.data_daily dd
        inner join public.inversor i on dd.id_i = i.id_i
        where dd.id_p = {self.id_p}
        and dd.energy_dd is not null and dd.energy_dd <> 0
        and dd.fecha_dd >= to_date('{self.t_start}', 'YYYY-MM-DD') AND dd.fecha_dd < to_date('{self.t_end}', 'YYYY-MM-DD')
        group by 1,2
        order by 1
        ;
        """
        df = self.db_connector.get_data_from_db(query_to_extract)
        return df
