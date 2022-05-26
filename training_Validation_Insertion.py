from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from application_logging import logger

class train_validation:
    def __init__(self,path):
        self.raw_data = Raw_Data_validation(path)
        self.dBOperation = dBOperation()
        self.db='training_logs'
        self.collection='Training_Main_Log'
        self.log_writer = logger.App_Logger()

    def train_validation(self):
        try:
            self.log_writer.log(self.db, self.collection, 'INFO', 'Start of Validation on files for prediction!!')
            # extracting values from training schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of training files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            # validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.db, self.collection, 'INFO', "Raw Data Validation Complete!!")



            self.log_writer.log(self.db, self.collection, 'INFO', "Insertion of Data into Collection started!!!!")
            # insert csv files in the collection
            self.dBOperation.insertIntoCollectionGoodData('good_raw_data')
            self.log_writer.log(self.db, self.collection, 'INFO', "Insertion in Collection completed!!!")
            self.log_writer.log(self.db, self.collection, 'INFO', "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(self.db, self.collection, 'INFO', "Good_Data folder deleted!!!")
            self.log_writer.log(self.db, self.collection, 'INFO', "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.db, self.collection, 'INFO', "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.db, self.collection, 'INFO', "Validation Operation completed!!")
            self.log_writer.log(self.db, self.collection, 'INFO', "Extracting csv file from collection")
            # export data in table to csvfile
            self.dBOperation.selectingDatafromcollectionintocsv('good_raw_data')

        except Exception as e:
            raise e









