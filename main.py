import os
import json
from datetime import date

import requests
import pandas as pd

from utils import HEADERS, QUERY, ROOT_URL

class LeanderScraper:
    """Scrapes permits from the city of Leander and saves the result to csv"""
    def __init__(self) -> None:
        self.permits = []

    @staticmethod
    def __fetch_data() -> json:
        """Fetches json data from an api used by the City of Leander"""

        for _ in range(3):
            try:
                response = requests.post(ROOT_URL, 
                                         headers=HEADERS, json=QUERY)
                
                if response.status_code == 200:
                    return response.json()

            except:pass
    
    @staticmethod
    def __validate_data(json_data:json) -> bool:
        """Checks if an error message was encountered during the request
        
           Arg:
             - json_data: json data from the server
        """
        verify = {"ErrorMessage" : json_data["ErrorMessage"],
                  "ValidationErrorMessage" : json_data["ValidationErrorMessage"],
                  "ConcurrencyErrorMessage" : json_data["ConcurrencyErrorMessage"],
                  "StatusCode" : json_data["StatusCode"],
                  "BrokenRules" : json_data["BrokenRules"]}
        
        print(f"Complete logs {verify}")

        if verify["StatusCode"] == 200:
            return True
        else:
            return False
    
    @staticmethod
    def __extract_data_description(json_data:json) -> dict:
        """Extracts the total number of pages and permits found
        
           Arg:
             - json_data: json data from the server
           Returns:
             - dictionary containing total pages and permits
        """
        return {"TotalPages": json_data["Result"]["TotalPages"],
                "TotalPermits": json_data["Result"]["TotalFound"]}
    
    @staticmethod
    def __extract_permits(json_data:json) -> list[dict]:
        """Extracts the permit slugs from a json returned from a request

           Arg:
             - json_data: json containing the permit slugs
           Return:
             - permits: a list containing all the permits extracted
        """
        permits = []

        for permit in json_data["Result"]["EntityResults"]: 
            try:
                address = permit["Address"]["FullAddress"]
            except:
                address = ""

            permits.append(
                {
                    "CaseID" : permit["CaseId"],
                    "CaseNumber" : permit["CaseNumber"],
                    "CaseTypeId" : permit["CaseTypeId"],
                    "CaseType" : permit["CaseType"],
                    "CaseWorkclassId" : permit["CaseWorkclassId"],
                    "CaseWorkclass" : permit["CaseWorkclass"],
                    "CaseStatusId" : permit["CaseStatusId"],
                    "CaseStatus" : permit["CaseStatus"],
                    "DateApplied" : permit["ApplyDate"],
                    "Address" : address,
                    "Description" : permit["Description"]
                }
            )
        
        return permits
    
    def save_to_csv(self) -> None:
        """Saves resulting data to csv"""
        if not os.path.exists("./data/"):os.makedirs("./data/")

        df = pd.DataFrame.from_dict(self.permits)

        df.to_csv("./data/permits_data_{}.csv".format(date.today()), index=False)
    
    def run(self):
        data = self.__fetch_data()

        data_desc = self.__extract_data_description(data)

        for page_num in range(1, data_desc["TotalPages"]):
            QUERY['PageNumber'] = page_num

            data = self.__fetch_data()

            is_valid = self.__validate_data(data)

            if is_valid:
                self.permits.extend(self.__extract_permits(data))
            
            self.save_to_csv()

if __name__ == "__main__":
    scraper = LeanderScraper()
    scraper.run()