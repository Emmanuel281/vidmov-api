1. go to baseapp folder
    cd baseapp/
2. create a virtual environment on current folder
    virtualenv -p python3 venv
    python3.12 -m venv venv
3. active the virtual environment
    source bin/activate
    source venv/bin/activate
4. install depencency
    pip install -r requirements.txt
5. upgrade python library
    pip install --upgrade -r requirements.txt

<!-- RUN APPS -->
directly:
    uvicorn baseapp.app:app --host 0.0.0.0 --port 1899 --reload

to run on docker:
<!-- Without docker compose -->
1. docker build -t api-baseapp .
2. docker run -d --name api-baseapp --network dbr0 --ip 172.1.0.12 -p 17177:1899 -v /dvol/build/source/baseapp-v2/baseapp:/app/baseapp api-baseapp:latest

<!-- With docker compose -->
1. docker-compose up --build -d

to run consumer (rabbitmq):
1. python -m baseapp.services.consumer --queue {queue_name}