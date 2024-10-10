from constants.CONSTANT import CONSTANT
from models.SQLDatabases.Comparison.ForeignKeyComparison import ForeignKeyComparison
from models.SQLDatabases.Comparison.PrimaryKeyComparison import PrimaryKeyComparison
from models.SQLDatabases.Comparison.UniqueKeyComparison import UniqueKeyComparison


class ConstraintComparison:

    def __init__(self,a,b):
        primary = PrimaryKeyComparison(a[CONSTANT.TABLE.PRIMARY_KEY],b[CONSTANT.TABLE.PRIMARY_KEY]).response
        foreign = ForeignKeyComparison(a[CONSTANT.TABLE.FOREIGN_KEY],b[CONSTANT.TABLE.FOREIGN_KEY]).response
        unique  = UniqueKeyComparison(a[CONSTANT.TABLE.UNIQUE_KEY],b[CONSTANT.TABLE.UNIQUE_KEY]).response

        self.responseJSON  = {
            CONSTANT.TABLE.PRIMARY_KEY: primary,
            CONSTANT.TABLE.FOREIGN_KEY:foreign,
            CONSTANT.TABLE.UNIQUE_KEY:unique
        }

