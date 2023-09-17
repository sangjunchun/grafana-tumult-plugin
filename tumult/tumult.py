from pyspark import SparkFiles
from pyspark.sql import SparkSession
from tmlt.analytics.privacy_budget import PureDPBudget
from tmlt.analytics.protected_change import AddOneRow
from tmlt.analytics.query_builder import QueryBuilder
from tmlt.analytics.session import Session


spark = SparkSession.builder.getOrCreate()


def add_spark_context_file():
    spark.sparkContext.addFile(
        "https://tumult-public.s3.amazonaws.com/library-members.csv"
    )
    members_df = spark.read.csv(
        SparkFiles.get("library-members.csv"), header=True, inferSchema=True
    )
    return members_df


def configure_session(dataframe, source_id, budget):
    budget = PureDPBudget(epsilon=budget) # maximum budget consumed in the Session
    session = Session.from_dataframe(
        privacy_budget=budget,
        source_id=source_id,
        dataframe=dataframe,
        protected_change=AddOneRow(),
    )
    return session


def describe_session(session):
    session.describe()


def count_query(session, source_id, filter=None, epsilon=1, use_remaining_privacy_budget=False):
    query_builder = QueryBuilder(source_id)
    if filter is not None:
        query_builder = query_builder.filter(filter)
    count_query = query_builder.count()
    budget = PureDPBudget(epsilon=epsilon)

    if use_remaining_privacy_budget is True:
        budget = session.remaining_privacy_budget
    total_count = session.evaluate(
        count_query,
        privacy_budget=budget
    )
    return total_count


def get_remaining_privacy_budget(session):
    print(session.remaining_privacy_budget)


def getShowString(df, n=20, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return(df._jdf.showString(n, 20, vertical))
    else:
        return(df._jdf.showString(n, int(truncate), vertical))


if __name__ == "__main__":
    members_df = add_spark_context_file()
    session = configure_session(members_df, "members", 3)
    describe_session(session)
    count = count_query(session, "members", None, 1)
    count.show()
    print(count)
    # minor_count = count_query(session, "members", "age < 18", 1)
    # minor_count.show()
    # edu_count = count_query(session, "members", "education_level IN ('masters-degree', 'doctorate-professional')", 0, True)
    # edu_count.show()

