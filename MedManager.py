from util import *
from DataManager import *


class MedManager(DataManager):
    """
    MedManager class inherit DataManger process all the medication
    """

    def organize_df(self, inplace =False):
        """

        :param inplace:
        :return:
        """
        delta = 90
        glp = self.df.copy()
        gap_days = 'more_than_' + str(delta) + 'days'
        glp['time_diff'] = glp.groupby(['GRID'])['DRUG_EXPOSURE_START_DATE'].diff().fillna(
            pd.Timedelta('0 days'))  # fill Na with 0 days
        glp[gap_days] = (glp.time_diff > datetime.timedelta(days=delta)) | (glp.time_diff == datetime.timedelta(days=0))

        glp_selected = glp[glp['more_than_90days']].drop(columns=['more_than_90days', 'time_diff'])

        print('before merge, total records: ', len(glp), ' \ndata have the stop records:',
              sum(glp.DRUG_EXPOSURE_END_DATE.notna()), 4)
        print('------')
        print('after merge, total records: ', len(glp_selected), ' \ndata have the stop records:',
              sum(glp_selected.DRUG_EXPOSURE_END_DATE.notna()), 4)

        if inplace:
            self.df = glp_selected

        return glp_selected



    def pre_median(self,df1,df2, pre_range=[0, 6], start_date_col='DRUG_EXPOSURE_START_DATE', lab_date_col='LAB_DATE',
                   value_date_col='LAB_VALUE', primary_key='GRID'):
        """

        :param other:
        :param pre_range:
        :param start_date_col:
        :param lab_date_col:
        :param value_date_col:
        :param primary_key:
        :return:
        """

        delta = pre_range[1]
        shift_start = pre_range[0]

        col0 = 'start_date_' + '>'.join(map(str, pre_range)) + 'month'  # this is the end date for the time
        col1 = 'end_date' + '>'.join(map(str, pre_range)) + 'month'  # this is the deadline for the time
        col2 = 'in_pre_' + '>'.join(map(str, pre_range)) + 'month'  # this is the tage for whether it is in the range
        result_col = 'median_' + '>'.join(map(str, pre_range)) + 'month'

        merged_df = pd.merge(df1, df2, on=primary_key, how='outer')  # merge two table

        # generate two column, one is the new start one is the end
        merged_df[col0] = merged_df[start_date_col] + pd.DateOffset(months=shift_start)
        merged_df[col1] = merged_df[start_date_col] + pd.DateOffset(months=delta)

        # add a tag for whether a record is with in the previous delta time
        if delta <= 0:  # before medication include the same day
            merged_df[col2] = (merged_df[lab_date_col] <= merged_df[col0]) & (
                        merged_df[lab_date_col] >= merged_df[col1])
        else:  # after take medicine does not include the same day
            merged_df[col2] = (merged_df[lab_date_col] <= merged_df[col1]) & (merged_df[lab_date_col] > merged_df[col0])

        # obtain the median value
        temp = merged_df.groupby(['GRID', 'DRUG_EXPOSURE_START_DATE']).apply(
            lambda x: find_m(x[col2], x[value_date_col])).reset_index()
        temp.columns = [*temp.columns[:-1], result_col]
        median_df = pd.merge(df1, temp, on=[primary_key, start_date_col], how='inner')
        median_df.head(30)

        return median_df, temp, merged_df


