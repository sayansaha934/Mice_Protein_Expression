import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from sklearn.preprocessing import LabelEncoder
import pickle
import os


class Preprocessor:
    """
        This class shall  be used to clean and transform the data before training.

        Written By: Sayan Saha
        Version: 1.0
        Revisions: None

        """

    def __init__(self, db, collection, logger_object):
        self.db=db
        self.collection=collection
        self.logger_object = logger_object


    def separate_label_feature(self, data, label_column_name):
        """
                        Method Name: separate_label_feature
                        Description: This method separates the features and a Label Coulmns.
                        Output: Returns two separate Dataframes, one containing features and the other containing Labels .
                        On Failure: Raise Exception

                        Written By:Sayan Saha
                        Version: 1.0
                        Revisions: None

                """
        self.logger_object.log(self.db, self.collection, 'INFO', 'Entered the separate_label_feature method of the Preprocessor class')
        try:
            self.X=data.drop(labels=label_column_name,axis=1) # drop the columns specified and separate the feature columns
            self.Y=data[label_column_name] # Filter the Label columns
            self.logger_object.log(self.db, self.collection, 'INFO', 'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            return self.X, self.Y
        except Exception as e:
            self.logger_object.log(self.db, self.collection, 'ERROR','Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.db, self.collection, 'ERROR', 'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise e

    def dropUnnecessaryColumns(self,data,columnNameList):
        """
                        Method Name: is_null_present
                        Description: This method drops the unwanted columns as discussed in EDA section.

                        Written By: Sayan Saha
                        Version: 1.0
                        Revisions: None

                                """
        data = data.drop(columnNameList,axis=1)
        return data




    def is_null_present(self,data):
        """
                                Method Name: is_null_present
                                Description: This method checks whether there are null values present in the pandas Dataframe or not.
                                Output: Returns a Boolean Value. True if null values are present in the DataFrame, False if they are not present.
                                On Failure: Raise Exception

                                Written By: Sayan Saha
                                Version: 1.0
                                Revisions: None

                        """
        self.logger_object.log(self.db, self.collection, 'INFO', 'Entered the is_null_present method of the Preprocessor class')
        self.null_present = False
        try:
            self.null_counts=data.isna().sum() # check for the count of null values per column
            for i in self.null_counts:
                if i>0:
                    self.null_present=True
                    break
            if(self.null_present): # write the logs to see which columns have null values
                dataframe_with_null = pd.DataFrame()
                dataframe_with_null['columns'] = data.columns
                dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                preprocessing_folder='preprocessing_data/'
                if not os.path.isdir(preprocessing_folder):
                    os.mkdir(preprocessing_folder)
                dataframe_with_null.to_csv(preprocessing_folder+'null_values.csv', header=True, index=None) # storing the null column information to file
            self.logger_object.log(self.db, self.collection, 'INFO','Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
            return self.null_present
        except Exception as e:
            self.logger_object.log(self.db, self.collection, 'ERROR','Exception occured in is_null_present method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.db, self.collection, 'ERROR','Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise e

    def encodeCategoricalValues(self,data):
     """
                                        Method Name: encodeCategoricalValues
                                        Description: This method encodes all the categorical values in the training set.
                                        Output: A Dataframe which has all the categorical values encoded.
                                        On Failure: Raise Exception

                                        Written By: Sayan Saha
                                        Version: 1.0
                                        Revisions: None
                     """

     # We can map the categorical values like below:
     data['Genotype'] = data['Genotype'].map({'Control': 0, 'Ts65Dn': 1})
     data['Treatment'] = data['Treatment'].map({'Saline': 0, 'Memantine': 1})
     data['Behavior'] = data['Behavior'].map({'C/S': 0, 'S/C': 1})


     encode = LabelEncoder().fit(data['class'])

     data['class'] = encode.transform(data['class'])


    # we will save the encoder as pickle to use when we do the prediction. We will need to decode the predcited values
    # back to original
     encoder_folder='EncoderPickle/'
     if not os.path.isdir(encoder_folder):
         os.mkdir(encoder_folder)
     with open(encoder_folder+'enc.pickle', 'wb') as file:
         pickle.dump(encode, file)



     return data


    def encodeCategoricalValuesPrediction(self,data):
        """
                                               Method Name: encodeCategoricalValuesPrediction
                                               Description: This method encodes all the categorical values in the prediction set.
                                               Output: A Dataframe which has all the categorical values encoded.
                                               On Failure: Raise Exception

                                               Written By: Sayan Saha
                                               Version: 1.0
                                               Revisions: None
                            """

         # We can map the categorical values like below:
        data['Genotype'] = data['Genotype'].map({'Control': 0, 'Ts65Dn': 1})
        data['Treatment'] = data['Treatment'].map({'Saline': 0, 'Memantine': 1})
        data['Behavior'] = data['Behavior'].map({'C/S': 0, 'S/C': 1})

        return data


    def impute_missing_values(self, data):
        """
                                        Method Name: impute_missing_values
                                        Description: This method replaces all the missing values in the Dataframe using KNN Imputer.
                                        Output: A Dataframe which has all the missing values imputed.
                                        On Failure: Raise Exception

                                        Written By: Sayan Saha
                                        Version: 1.0
                                        Revisions: None
                     """
        self.logger_object.log(self.db, self.collection, 'INFO', 'Entered the impute_missing_values method of the Preprocessor class')
        self.data= data
        try:
            imputer=KNNImputer(n_neighbors=3, weights='uniform',missing_values=np.nan)
            self.new_array=imputer.fit_transform(self.data) # impute the missing values
            # convert the nd-array returned in the step above to a Dataframe
            self.new_data=pd.DataFrame(data=self.new_array, columns=self.data.columns)
            self.logger_object.log(self.db, self.collection, 'INFO', 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
            return self.new_data
        except Exception as e:
            self.logger_object.log(self.db, self.collection, 'ERROR','Exception occured in impute_missing_values method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.db, self.collection, 'ERROR','Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
            raise e

