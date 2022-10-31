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

    def upper_quartile(self, col_nm: str) -> numbers.Number:
        df_summary = self._df.select(col_nm).summary()
        rslt: numbers.Number = df_summary[df_summary["summary"]=='75%'].select(col_nm).collect()[0][0]
        return rslt

    def upper_bound(self, col_nm: str, sensitivity: numbers.Number = 1.5) -> numbers.Number:
        uq = self.upper_quartile(col_nm)
        lq = self.lower_quartile(col_nm)
        iqr: numbers.Number = uq-lq
        return self.median(col_nm) + iqr * sensitivity
