import pandas as pd
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from application_logging import logger
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
import pickle


class prediction:

    def __init__(self,path):
        self.db='prediction_logs'
        self.collection='Prediction_Log'
        self.log_writer = logger.App_Logger()
        self.pred_data_val = Prediction_Data_validation(path)

    def predictionFromModel(self):

        try:
            self.pred_data_val.createPredictionOutputFolder()
            self.pred_data_val.deletePredictionFile() #deletes the existing prediction file from last run!
            self.log_writer.log(self.db, self.collection, 'INFO', 'Start of Prediction')
            data_getter=data_loader_prediction.Data_Getter_Pred(self.db, self.collection, self.log_writer)
            data=data_getter.get_data()

            MouseIDs=data['MouseID']
            preprocessor=preprocessing.Preprocessor(self.db, self.collection,self.log_writer)
            data = preprocessor.dropUnnecessaryColumns(data, ['MouseID'])



            # get encoded values for categorical data

            data = preprocessor.encodeCategoricalValuesPrediction(data)
            is_null_present=preprocessor.is_null_present(data)
            if(is_null_present):
                data=preprocessor.impute_missing_values(data)

            file_loader=file_methods.File_Operation(self.db, self.collection, self.log_writer)
            kmeans=file_loader.load_model('KMeans')


            clusters=kmeans.predict(data)#drops the first column for cluster prediction
            data['clusters']=clusters
            clusters=data['clusters'].unique()
            result=[] # initialize balnk list for storing predicitons
            with open('EncoderPickle/enc.pickle', 'rb') as file: #let's load the encoder pickle file to decode the values
                encoder = pickle.load(file)

            data['MouseID']=MouseIDs
            for i in clusters:
                cluster_data= data[data['clusters']==i]
                mouse_ids=cluster_data['MouseID']
                cluster_data = cluster_data.drop(['clusters', 'MouseID'],axis=1)
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                predictions=encoder.inverse_transform(model.predict(cluster_data).astype(int))
                for mouse_id, val in zip(mouse_ids, predictions):
                    result.append({'MouseID': mouse_id, 'class': val})
            result = pd.DataFrame(result)
            path="Prediction_Output_File/Predictions.csv"
            result.to_csv(path,header=True, index=None) #appends result to prediction file
            self.log_writer.log(self.db, self.collection, 'INFO','End of Prediction')
        except Exception as e:
            self.log_writer.log(self.db, self.collection, 'ERROR', 'Error occured while running the prediction!! Error:: %s' % e)
            raise e
        return path





