import pickle
import os
import shutil


class File_Operation:
    """
                This class shall be used to save the model after training
                and load the saved model for prediction.

                Written By: Sayan Saha
                Version: 1.0
                Revisions: None

                """
    def __init__(self,db, collection, logger_object):
        self.db=db
        self.collection=collection
        self.logger_object = logger_object
        self.model_directory='models/'

    def save_model(self,model,filename):
        """
            Method Name: save_model
            Description: Save the model file to directory
            Outcome: File gets saved
            On Failure: Raise Exception

            Written By: Sayan Saha
            Version: 1.0
            Revisions: None
"""
        self.logger_object.log(self.db, self.collection, 'INFO', 'Entered the save_model method of the File_Operation class')
        try:
            path = os.path.join(self.model_directory,filename) #create seperate directory for each cluster
            if os.path.isdir(path): #remove previously existing models for each clusters
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path) #
            with open(path +'/' + filename+'.sav', 'wb') as f:
                pickle.dump(model, f) # save the model to file
            self.logger_object.log(self.db, self.collection, 'INFO', 'Model File '+filename+' saved. Exited the save_model method of the Model_Finder class')

            return 'success'
        except Exception as e:
            self.logger_object.log(self.db, self.collection, 'ERROR','Exception occured in save_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.db, self.collection, 'ERROR', 'Model File '+filename+' could not be saved. Exited the save_model method of the Model_Finder class')
            raise e

    def load_model(self,filename):
        """
                    Method Name: load_model
                    Description: load the model file to memory
                    Output: The Model file loaded in memory
                    On Failure: Raise Exception

                    Written By: Sayan Saha
                    Version: 1.0
                    Revisions: None
        """
        self.logger_object.log(self.db, self.collection, 'INFO', 'Entered the load_model method of the File_Operation class')
        try:
            with open(self.model_directory + filename + '/' + filename + '.sav',
                      'rb') as f:
                self.logger_object.log(self.db, self.collection, 'INFO',
                                       'Model File ' + filename + ' loaded. Exited the load_model method of the Model_Finder class')
                return pickle.load(f)
        except Exception as e:
            self.logger_object.log(self.db, self.collection, 'ERROR',
                                   'Exception occured in load_model method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.db, self.collection, 'ERROR',
                                   'Model File ' + filename + ' could not be saved. Exited the load_model method of the Model_Finder class')
            raise e

    def find_correct_model_file(self,cluster_number):
        """
                            Method Name: find_correct_model_file
                            Description: Select the correct model based on cluster number
                            Output: The Model file
                            On Failure: Raise Exception

                            Written By: Sayan Saha
                            Version: 1.0
                            Revisions: None
                """
        self.logger_object.log(self.db, self.collection, 'INFO', 'Entered the find_correct_model_file method of the File_Operation class')
        try:
            self.cluster_number= cluster_number
            self.folder_name=self.model_directory
            # self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    if (self.file.index(str( self.cluster_number))!=-1):
                        self.model_name=self.file
                except:
                    continue
            # self.model_name=self.model_name.split('.')[0]
            self.logger_object.log(self.db, self.collection, 'INFO',
                                   'Exited the find_correct_model_file method of the Model_Finder class.')
            return self.model_name
        except Exception as e:
            self.logger_object.log(self.db, self.collection, 'ERROR',
                                   'Exception occured in find_correct_model_file method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.db, self.collection, 'ERROR',
                                   'Exited the find_correct_model_file method of the Model_Finder class with Failure')
            raise e