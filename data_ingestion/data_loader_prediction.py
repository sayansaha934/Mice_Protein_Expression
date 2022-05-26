import pandas as pd

class Data_Getter_Pred:
    """
    This class shall  be used for obtaining the data from the source for prediction.

    Written By: Sayan Saha
    Version: 1.0
    Revisions: None

    """
    def __init__(self, db, table_name, logger_object):
        self.prediction_file='Prediction_FileFromDB/InputFile.csv'
        self.db=db
        self.table_name=table_name
        self.logger_object=logger_object

    def get_data(self):
        """
        Method Name: get_data
        Description: This method reads the data from source.
        Output: A pandas DataFrame.
        On Failure: Raise Exception

         Written By: Sayan Saha
        Version: 1.0
        Revisions: None

        """
        self.logger_object.log(self.db, self.table_name, 'INFO', 'Entered the get_data method of the Data_Getter class')
        try:
            self.data= pd.read_csv(self.prediction_file) # reading the data file
            self.logger_object.log(self.db, self.table_name, 'INFO','Data Load Successful.Exited the get_data method of the Data_Getter class')
            return self.data
        except Exception as e:
            self.logger_object.log(self.db, self.table_name, 'ERROR','Exception occured in get_data method of the Data_Getter class. Exception message: '+str(e))
            self.logger_object.log(self.db, self.table_name, 'ERROR',
                                   'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise e


