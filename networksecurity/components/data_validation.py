from networksecurity.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from scipy.stats import ks_2samp
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file
import pandas as pd
import os,sys 

class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact,
                        data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    def validate_number_of_columns(self, dataframe:pd.DataFrame)->bool:
        try: 
            number_of_columns = len(self._schema_config['columns'])
            logging.info(f"Required number of columns: {number_of_columns}")
            logging.info(f"Data frame has columns: {len(dataframe.columns)}")
            if len(dataframe.columns) == number_of_columns:
                return True
            return False
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def detect_dataset_drift(self, base_df, current_df, threshold=0.01, max_drift_columns=2) -> bool:
        try:
            status=True
            report={}
            drift_count = 0
            for column in base_df.columns:
                d1=base_df[column]
                d2=current_df[column]
                is_sample_dist = ks_2samp(d1,d2)
                if threshold<=is_sample_dist.pvalue:
                    is_found=False
                else:
                    is_found=True
                    drift_count += 1
                report.update({column:{
                    "p_value":float(is_sample_dist.pvalue),
                    "drift_status": is_found
                }})
            
            # Allow some drift but not too much
            if drift_count > max_drift_columns:
                status = False
                logging.warning(f"Data drift detected in {drift_count} columns. Maximum allowed: {max_drift_columns}")
            elif drift_count > 0:
                logging.info(f"Minor data drift detected in {drift_count} columns, but within acceptable limits")
            
            drift_report_file_path = self.data_validation_config.drift_report_file_path

            #Create Directory
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path, content=report)
            return status
        except Exception as e:
            raise NetworkSecurityException(e,sys)

    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

        # Read data
            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)

        # Initialize error message
            error_message = ""
            validation_status = True

        # Validate number of columns
            status = self.validate_number_of_columns(dataframe=train_dataframe)
            if not status:
                error_message = "Train dataframe does not contain all columns.\n"
                validation_status = False

        # Check data drift
            drift_status = self.detect_dataset_drift(base_df=train_dataframe, current_df=test_dataframe)
            if not drift_status:
                error_message += "Data drift detected between train and test data.\n"
                validation_status = False

        # Create directories if they don't exist
            dir_path = os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path, exist_ok=True)

        # Save validated data
            train_dataframe.to_csv(
                self.data_validation_config.valid_train_file_path, index=False, header=True
            )
            test_dataframe.to_csv(
                self.data_validation_config.valid_test_file_path, index=False, header=True
            )

        # Prepare artifact
            data_validation_artifact = DataValidationArtifact(
                validation_status=validation_status,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )

            if error_message:
                raise Exception(error_message)

            return data_validation_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)


