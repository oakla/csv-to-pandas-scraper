# Instructions:

**1. Make sure `pip` is installed:**

Check to see if `pip` is installed:

`python -m pip --version`

If pip is installed, continue to the next step. Otherwise, install `pip` with


`curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py`

`python3 get-pip.py` --or-- `python get-pip.py`

**2. Install `virtualenv`**

`pip install virtualenv`


**3. Create a virtualenv:**

From the directory that this repo was downloaded to, run:

`python3 -m venv venv` --or-- `virtualenv venv`

**4. Activate the virtualenv:**

`source venv/bin/activate`

**5. Install the required libs:**

`pip install -r requirements.txt`

**6. Run the script:**

`python3 main.py` --or just-- `python main.py`