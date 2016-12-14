import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal
from pyspark import sql

from pysparkhelpers import helpers


class TestUdaf(object):

    @pytest.mark.usefixtures("hive_context")
    def test_single(self, hive_context):
        df = pd.DataFrame({'user': ['a', 'a', 'a', 'b'],
                           'values': [1, 1, 1, 4]})
        sdf = hive_context.createDataFrame(df).cache()

        def func(x):
            return x.sum()

        expected = pd.DataFrame({'user': ['a', 'b'], 'values': [3, 4]})
        returned_sdf = (
            helpers.udaf('user', func, sdf).toPandas()
            .sort_values('user')
            .reset_index(drop=True)
        )
        assert_frame_equal(expected, returned_sdf)
        returned_rdd = (
            helpers.udaf('user', func, sdf.rdd).toPandas()
            .sort_values('user')
            .reset_index(drop=True)
        )
        assert_frame_equal(expected, returned_rdd)

        def func_value(x):
            return 3

        expected_value = pd.DataFrame({'user': ['a', 'b'], 'value': [3, 3]})
        returned_value = (
            helpers.udaf('user', func_value, sdf).toPandas()
            .sort_values('user')
            .reset_index(drop=True)
        )
        assert_frame_equal(expected_value, returned_value)

        def func_kwargs(x, addition):
            s = x.sum()
            s['values'] += addition
            return s

        expected_kwargs = pd.DataFrame({'user': ['a', 'b'], 'values': [5, 6]})
        returned_kwargs = (
            helpers.udaf('user', func_kwargs, sdf, addition=2).toPandas()
            .sort_values('user')
            .reset_index(drop=True)
        )
        assert_frame_equal(expected_kwargs, returned_kwargs)

    @pytest.mark.usefixtures("hive_context")
    def test_multiple(self, hive_context):
        df_a = pd.DataFrame({'user': ['a', 'a', 'a', 'b'],
                            'values': [1, 1, 1, 4]})
        df_b = pd.DataFrame({'user': ['a', 'b'],
                             'multiplier': [1, -1]})
        sdf_a = hive_context.createDataFrame(df_a).cache()
        sdf_b = hive_context.createDataFrame(df_b).cache()

        def func(first, second):
            return first.sum() * second['multiplier'].values

        expected = pd.DataFrame({'user': ['a', 'b'], 'values': [3, -4]})
        returned_sdf = (
            helpers.udaf('user', func, sdf_a, sdf_b).toPandas()
            .sort_values('user')
            .reset_index(drop=True)
        )
        assert_frame_equal(expected, returned_sdf)
        returned_rdd = (
            helpers.udaf('user', func, sdf_a.rdd, sdf_b.rdd).toPandas()
            .sort_values('user')
            .reset_index(drop=True)
        )
        assert_frame_equal(expected, returned_rdd)

    @pytest.mark.usefixtures("hive_context")
    def test_multiple_by(self, hive_context):
        df_a = pd.DataFrame({'user': ['a', 'a', 'a', 'b', 'b'],
                             'member': ['yes', 'yes', 'no', 'yes', 'no'],
                             'values': [1, 1, 1, 4, 5]})
        df_b = pd.DataFrame({'user': ['a', 'a', 'b'],
                             'member': ['yes', 'no', 'yes'],
                             'multiplier': [1, 0, -1]})
        sdf_a = hive_context.createDataFrame(df_a)
        sdf_b = hive_context.createDataFrame(df_b)

        def func(first, second):
            return first.sum() * second['multiplier'].values

        expected = pd.DataFrame({'user': ['a', 'a', 'b'],
                                 'member': ['no', 'yes', 'yes'],
                                 'values': [0, 2, -4]})
        returned_sdf = (
            helpers.udaf(['user', 'member'], func, sdf_a, sdf_b).toPandas()
            .sort_values(['user', 'member'])
            .reset_index(drop=True)
        )
        assert_frame_equal(expected, returned_sdf)
        returned_rdd = (
            helpers.udaf(['user', 'member'], func, sdf_a.rdd,
                         sdf_b.rdd).toPandas()
            .sort_values(['user', 'member'])
            .reset_index(drop=True)
        )
        assert_frame_equal(expected, returned_rdd)

    def test_to_rows(self):
        df = pd.Series(None)
        expected = [sql.Row(user='a')]
        returned = helpers._to_rows(df, 'user', 'a')
        assert [x.asDict() for x in expected] == [x.asDict() for x in returned]

        df = pd.Series({'value': 1})
        expected = [sql.Row(user='a', value=1)]
        returned = helpers._to_rows(df, 'user', 'a')
        assert [x.asDict() for x in expected] == [x.asDict() for x in returned]

        df = pd.Series({'value': 1, 'category': 'c'})
        expected = [sql.Row(category='c', user='a', value=1)]
        returned = helpers._to_rows(df, 'user', 'a')
        assert [x.asDict() for x in expected] == [x.asDict() for x in returned]

        df = pd.DataFrame({'value': [3, 1]})
        expected = [sql.Row(user='a', value=3), sql.Row(user='a', value=1)]
        returned = helpers._to_rows(df, 'user', 'a')
        assert [x.asDict() for x in expected] == [x.asDict() for x in returned]

        expected = [sql.Row(user='a', value=1)]
        returned = helpers._to_rows(1, 'user', 'a')
        assert [x.asDict() for x in expected] == [x.asDict() for x in returned]

        df = pd.Series({'value': 1})
        expected = [sql.Row(group=1, user='a', value=1)]
        returned = helpers._to_rows(df, ['user', 'group'], ['a', 1])
        assert [x.asDict() for x in expected] == [x.asDict() for x in returned]

        expected = [sql.Row(group=1, user='a', value=1)]
        returned = helpers._to_rows(1, ['user', 'group'], ['a', 1])
        assert [x.asDict() for x in expected] == [x.asDict() for x in returned]
