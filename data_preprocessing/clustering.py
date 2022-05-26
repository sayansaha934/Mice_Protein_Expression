import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operations import file_methods

class KMeansClustering:
    """
            This class shall  be used to divide the data into clusters before training.

            Written By: Sayan Saha
            Version: 1.0
            Revisions: None

            """

    def __init__(self, db, collection, logger_object):
        self.db=db
        self.collection=collection
        self.logger_object = logger_object

    def elbow_plot(self,data):
        """
                        Method Name: elbow_plot
                        Description: This method saves the plot to decide the optimum number of clusters to the file.
                        Output: A picture saved to the directory
                        On Failure: Raise Exception

                        Written By: Sayan Saha
                        Version: 1.0
                        Revisions: None

                """
        self.logger_object.log(self.db, self.collection, 'INFO', 'Entered the elbow_plot method of the KMeansClustering class')
        wcss=[] # initializing an empty list
        try:
            for i in range (1,11):
                kmeans=KMeans(n_clusters=i,init='k-means++',random_state=42) # initializing the KMeans object
                kmeans.fit(data) # fitting the data to the KMeans Algorithm
                wcss.append(kmeans.inertia_)
            plt.plot(range(1,11),wcss) # creating the graph between WCSS and the number of clusters
            plt.title('The Elbow Method')
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            #plt.show()
            plt.savefig('preprocessing_data/K-Means_Elbow.PNG') # saving the elbow plot locally
            # finding the value of the optimum cluster programmatically
            self.kn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            self.logger_object.log(self.db, self.collection, 'INFO', 'The optimum number of clusters is: '+str(self.kn.knee)+' . Exited the elbow_plot method of the KMeansClustering class')
            return self.kn.knee

        except Exception as e:
            self.logger_object.log(self.db, self.collection, 'ERROR','Exception occured in elbow_plot method of the KMeansClustering class. Exception message:  ' + str(e))
            self.logger_object.log(self.db, self.collection, 'ERROR','Finding the number of clusters failed. Exited the elbow_plot method of the KMeansClustering class')
            raise e

    def create_clusters(self,data,number_of_clusters):
        """
                                Method Name: create_clusters
                                Description: Create a new dataframe consisting of the cluster information.
                                Output: A datframe with cluster column
                                On Failure: Raise Exception

                                Written By: Sayan Saha
                                Version: 1.0
                                Revisions: None

                        """
        self.logger_object.log(self.db, self.collection, 'INFO', 'Entered the create_clusters method of the KMeansClustering class')
        self.data=data
        try:
            self.kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', random_state=42)
            self.y_kmeans=self.kmeans.fit_predict(data) #  divide data into clusters

            self.file_op = file_methods.File_Operation(self.db, self.collection, self.logger_object)
            self.save_model = self.file_op.save_model(self.kmeans, 'KMeans') # saving the KMeans model to directory
                                                                                    # passing 'Model' as the functions need three parameters

            self.data['Cluster']=self.y_kmeans  # create a new column in dataset for storing the cluster information
            self.logger_object.log(self.db, self.collection, 'INFO', 'succesfully created '+str(self.kn.knee)+ 'clusters. Exited the create_clusters method of the KMeansClustering class')
            return self.data
        except Exception as e:
            self.logger_object.log(self.db, self.collection, 'ERROR','Exception occured in create_clusters method of the KMeansClustering class. Exception message:  ' + str(e))
            self.logger_object.log(self.db, self.collection, 'ERROR','Fitting the data to clusters failed. Exited the create_clusters method of the KMeansClustering class')
            raise e