1. go to apps folder
    cd vidmov-api/
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

to run application:
    uvicorn baseapp.app:app --host 0.0.0.0 --port 1899 --reload

to run on docker:
1. docker build -t api-reimburse .
2. docker run -d --name api-vidmov --network dbr0 --ip 172.1.0.12 -p 17177:1899 -v /dvol/build/source/vidmov-api/baseapp:/app/baseapp api-vidmov:latest