# streaming-examples
Streaming Cookbook

To create the quix.yaml file

```quix init```

To create a local pipeline

```quix local pipeline init```

Running the above will create a compose.yaml file and spin up containers to run kafka locally and RedPanda(a webui to view kakfa messages)

Now, we can create producers / subscribers / demo data sources boilerplate with quix

```quix local apps create```

Running this  will give us a list of options to create various boilerplate/template items