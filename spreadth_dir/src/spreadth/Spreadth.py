import numbers
import pyspark

class Spreadth:

    def __init__(self, df: pyspark.sql.DataFrame):
        self._df = df


    def median(self, col_nm: str) -> numbers.Number:
        df_summary = self._df.select(col_nm).summary()
        rslt: numbers.Number = df_summary[df_summary["summary"]=='50%'].select(col_nm).collect()[0][0]
        return rslt

    def lower_quartile(self, col_nm: str) -> numbers.Number:
        df_summary = self._df.select(col_nm).summary()
        rslt: numbers.Number = df_summary[df_summary["summary"]=='25%'].select(col_nm).collect()[0][0]
        return rslt
    