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