# zg-analytics-engine-python

## install

`sudo apt install python3.7 python3-venv python3.7-venv`
### or
`pip3 install virtualenv --user`

## create env
`python3 -m venv venv`

### or 
`python3 -m virtualenv venv`

`source src/venv/bin/activate`

## install lib

`pip install -r requirements.txt`

## generate lib

`pip freeze > requirements.txt`

## exit from venv
`deactivate`


## install lib with poetry
`poetry add lib_name`

## update it to the latest compatible version
`poetry update package`

## install lib with version
`poetry add pendulum@^2.0.5`
`poetry add "pendulum>=2.0.5" `

## install lib with pip via poetry
`poetry run python3 -m pip install  robotframework`

## common file function init project
`1)func folder > __init__.py , 2)constant.py, 3)setting folder, 4)definitions.py, `