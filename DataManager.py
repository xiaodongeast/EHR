
import sys, os, io
from util import *
from datetime import datetime


pd.set_option('display.max_row', 100)
pd.set_option('display.max_columns', 100)
sys.path.append('~/BioVu/SDJuly/glp1_patient')


class DataManager(object):
    __slots__ = 'df', 'encounter', 'log'

    def __init__(self):
        self.df = None
        self.encounter = None  # maybe not encounter, I do not really keep track encounter
        self.log = None

    # remember to change date_col to a list, and then add a sort_col to sort
    def open_csv(self, file_path, date_col=None, drop_col=None, pid='GRID', log_prefix='log_'):
        """
        This will not update encounter, I will need to use another one to set the encounter table
        Otherwise, time wasted
        :param file_path:
        :param date_col:
        :param drop_col:
        :param pid:
        :param log_prefix:
        :return:
        """
        try:
            self.df = self._read_df(file_path, date_col, drop_col, pid)
        except Exception as e:
            print('Fail to load the data! use help(open_csv) for usage')
        #self.encounter = self._create_encounter(self.df, grouping_field_list=[pid])

        # generate the path
        file_path_base, filename_base = os.path.split(file_path)
        filename_log = log_prefix + filename_base
        log_path = os.path.join(file_path_base, filename_log)

        # if this is the first or if the log not exist
        try:
            self.log = pd.read_csv(log_path)
        except Exception as e:
            print('an empty log file is created for the CSV')
            msg = 'Open:' + filename_base + ', created a new log'
            msg_time = datetime.now().strftime("%d-%m-%Y  %H:%M")
            self.log = pd.DataFrame(np.array([msg_time,filename_base,msg]).reshape(1,3), columns=['date', 'source_file','activity'])

    def save_csv(self, file_path, log_prefix='log_', encounter_prefix='encounter', encounter = False):
        """
        save the data as a csv file. The log file will added
        :param file_path: this include the base file path
        :param log_prefix:
        :param encounter_prefix:
        :param encounter:
        :return:
        """
        file_path_base, filename_base = os.path.split(file_path)

        filename_encounter = encounter_prefix + filename_base
        encounter_path = os.path.join(file_path_base, filename_encounter)

        filename_log = log_prefix + filename_base
        log_path = os.path.join(file_path_base, filename_log)

        try:
            self.df.to_csv(file_path, index = False)
            self.log.to_csv(log_path, index = False)
        except Exception as e:
            print(e)
            print('Fail to save! help(save_csv) for usage')

        if encounter:
            self.encounter.to_csv(encounter_path)

    def set_df(self, df, msg):
        """
        This method will be used to set the data directly. If self.log is empty
        :param df:
        :param msg:
        :return:
        """
        self.df = df
        previous_name = 'directly set the date'
        msg_time = datetime.now().strftime("%d-%m-%Y  %H:%M")

        if self.log is None:
            print('no previous log file')
            self.log = pd.DataFrame(np.array([msg_time, previous_name, msg]).reshape(1, 3),
                                    columns=['date', 'source_file', 'activity'])
        else:
            self.log = self.log.append({'date':msg_time,'source_file':'no updating', 'activity': msg},ignore_index=True)

    def get_df(self):
        """

        :return: two data frames, first is the actual data, second is the log
        """
        return self.df, self.log, self.encounter

    def _read_df(self, file_path, date_col, drop_col = None, pid='GRID'):
        """
        :param file_path: string, folder+ filename
        :param date_col: this the Date you want to sort the value, string
        :param drop_col: if there is any column need to be droped. default is None
        :param pid:  Patient ID. In the SD data, every table has a primary key GRID
        :return:  a dataframe, at the same time the data frame will be added to a class instance of the list
        """
        df = pd.read_csv(file_path)

        # convert Date and sort
        df[date_col] = pd.to_datetime(df[date_col])
        df.sort_values(by=[pid, date_col], inplace=True, ascending=True)
        if drop_col:
            df.drop(columns=drop_col, inplace=True)
        return df

    def _create_encounter(self, df, grouping_field_list=['GRID'], inplace=False):
        """
        :param df: input dataframe
        :param grouping_field_list: this is a list used to create the encounter table. default is GRID
        :return: a new dataframe of the encounter table
        """
        non_grouped_field_list = [c for c in df.columns if c not in grouping_field_list]
        encounter_df = df.groupby(grouping_field_list)[non_grouped_field_list]. \
            agg(lambda x: list([y for y in x if y is not np.nan])).reset_index()

        if inplace:
            self.encounter = encounter_df

        return encounter_df

    def clean_all(self, df, col, min_value, max_value, inplace = False):
        """

        :param df:
        :param col:
        :param min_value:
        :param max_value:
        :param inplace:
        :return:
        """
        if inplace:
            msg_time = datetime.now().strftime("%d-%m-%Y  %H:%M")
            msg = 'only keep data in column: {} in :{} - {}'.format(col, min_value, max_value)
            self.log = self.log.append({'date':msg_time,'source_file':'no updating', 'activity': msg},ignore_index=True)
        # assert col
        pass

    def clean_encounter(self, df, col='LAB_VALUE', col2='maxLAB_VALUE', min_value=6.5, max_value=float('inf'), primary_key='GRID', inplace = False):
        """

        :param df:
        :param col:
        :param col2:
        :param min_value:
        :param max_value:
        :param primary_key:
        :param inplace:
        :return:
        """
        encounter = create_encounter(df)
        temp = min_max_diff(encounter, [col])
        temp_selected = temp[temp[col2] > min_value]
        df_selected = df[df.GRID.isin(temp_selected[primary_key])]
        if inplace:
            msg_time = datetime.now().strftime("%d-%m-%Y  %H:%M")
            msg = 'only keep data in column: {} in :{} - {}'.format(col2, min_value, max_value)
            self.log = self.log.append({'date': msg_time, 'source_file': 'no updating', 'activity': msg}, ignore_index=True)
            self.df = df_selected
        return df_selected

    def __str__(self):
        buffer = io.StringIO()
        self.df.info(buf=buffer)
        s = buffer.getvalue()
        msg = '''
        '----the log file is:
        {}'
        ------ the info is:
        {}
        ------ the description is 
        {}''' . format(self.log.to_string(),s, self.df.describe(include ='all').to_string())

        return msg



    def organize_df(self):
        """
        subclass need to implement
        :return:
        """
        raise NotImplementedError('abstract method need to be implemented')


if __name__ == '__main__':

    demo_address = '~/BioVu/SDJuly/Gannon_T2D_DEMO.csv'
    d = DataManager()
    d.open_csv(demo_address, 'DOB')
    demo,b,c = d.get_df()
    demo.head()
    d.save_csv('./test.csv')

    demo.drop(columns=demo.columns[7:-3], inplace=True)
    demo.head()
    d.set_df(demo, 'dropped columns with all diseases')
    d.save_csv('./test2.csv')
    print(d)





