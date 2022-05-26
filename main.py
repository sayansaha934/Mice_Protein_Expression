from flask import Flask, request, render_template, send_file
from flask import Response
import shutil
import os
from werkzeug.utils import secure_filename
from flask_cors import CORS, cross_origin
from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
from predictFromModel import prediction

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
CORS(app)


@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route("/predict", methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        if request.is_json:
            path = request.json['folderPath']

            pred_val = pred_validation(path) #object initialization

            pred_val.prediction_validation() #calling the prediction_validation function

            pred = prediction(path) #object initialization

            # predicting for dataset present in database
            path = pred.predictionFromModel()
            return Response("Prediction File created at %s!!!" % path)
        else:
            try:
                path = request.form['folderPath']
            except:
                files=request.files.getlist('files')
                if not os.path.isdir('Custom_Batch_Files'):
                    os.mkdir('Custom_Batch_Files')
                for file in files:
                    file.save(os.path.join('Custom_Batch_Files', file.filename))
                path='Custom_Batch_Files'

            pred_val = pred_validation(path) #object initialization

            pred_val.prediction_validation() #calling the prediction_validation function

            pred = prediction(path) #object initialization

            # predicting for dataset present in database
            path = pred.predictionFromModel()

            if os.path.isdir('Custom_Batch_Files'):
                shutil.rmtree('Custom_Batch_Files')

            # prepairing for final output folder
            output_folder='Final_Output_Folder'
            if not os.path.isdir(output_folder):
                os.mkdir(output_folder)
            shutil.move('Prediction_Output_File', output_folder)
            shutil.move('PredictionArchivedBadData', output_folder)

            shutil.make_archive(output_folder, 'zip', output_folder)
            shutil.rmtree(output_folder)
            return send_file(output_folder+'.zip', as_attachment=True)

    except Exception as e:
        return Response("Error Occurred! %s" %e)



@app.route("/train", methods=['POST'])
@cross_origin()
def trainRouteClient():

    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']
            train_valObj = train_validation(path) #object initialization

            train_valObj.train_validation()#calling the training_validation function


            trainModelObj = trainModel() #object initialization
            trainModelObj.trainingModel() #training the model for the files in the table

    except Exception as e:

        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")



if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
