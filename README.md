# Toyota Camry Processor

## Setup instructions
```
 docker compose up -d zookeeper kafka kafka-ui
 docker compose up -d --build dev   
```

Login to dev container
```
docker exec -it toyota_camry-dev-1 bash
```

Run data generator
```
To create a virtual env: 
python -m venv venv

source venv/bin/activate
pip install -r requirements.txt

python data_generator.py
```

Run main application
```
mvn spring-boot:run
```

Check that page_views are stored:
```
sqlite3 output/attributed_page_views.db

select * from attributed_page_views;
```
You should see expected output: 
