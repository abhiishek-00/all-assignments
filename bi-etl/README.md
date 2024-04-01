1. clone the project : ``` git clone https://github.com/abhiishek-00/all-assignments.git ``` 
2. open terminal here & go to bi-etl : ``` cd bi-etl ``` 
3. install requirements.txt : ``` pip3 install -r requirements.txt ``` 
4. run extract.py : ``` python3 extract.py ```  
5. run transform.py : ``` python3 transform.py ``` 
6. open load.py & change values of dbname, user & password under dbvalues
7. run load.py : ``` python3 load.py ```


#### docker
1. ```docker-compose -f docker-compose.yaml up -d```
2. to check postgresSQL container
   1. ```docker exec -it container-id-here bash```
   2.  ```psql -U reema ```
   3.  ``select * from eltclienttable where id = 'c2'; ```
   4. ``` exit ```
