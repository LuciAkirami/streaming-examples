# streaming-examples
Streaming Cookbook

To create the quix.yaml file

```quix init```

To create a local pipeline

```quix local pipeline up```

Running the above will create a compose.yaml file and spin up containers to run kafka locally and RedPanda(a webui to view kakfa messages)

Now, we can create producers / subscribers / demo data sources boilerplate with quix

```quix local apps create```

Running this  will give us a list of options to create various boilerplate/template items, select the demo data source and provide a name for the project. Here its demo-data. The folder includes a yaml file, demo-data.csv containing F1 Car Demo data, dockerfile, main.py and a requirements.txt file

Now install the required libraries through pip from the requirements.txt file via `pip install -r requirements.txt`