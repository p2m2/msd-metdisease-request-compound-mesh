# msd-metdisease-request-compound-mesh

## configuration

- https://github.com/eMetaboHUB/Forum-DiseasesChem/tree/master/config/release-2021/computation

## request files

- https://github.com/eMetaboHUB/Forum-DiseasesChem/blob/master/app/computation/SPARQL

## script
- https://github.com/eMetaboHUB/Forum-DiseasesChem/blob/master/app/computation/requesting_virtuoso.py
- https://github.com/eMetaboHUB/Forum-DiseasesChem/blob/master/app/computation/processing_functions.py


## prepare test

### Chebi

```sh 
pushd rdf/ebi/chebi/current_release/
wget https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.owl
popd
```
### Mesh

```sh 
pushd rdf/nlm/mesh/current_release/
wget https://nlmpubs.nlm.nih.gov/projects/mesh/rdf/mesh.nt
popd
```

``` 
/usr/local/share/spark/bin/spark-submit \
   --conf "spark.eventLog.enabled=true" \
   --conf "spark.eventLog.dir=file:///tmp/spark-events" \
   --executor-memory 1G \
   --num-executors 1 \
   --jars ./sansa-ml-spark_2.12-0.8.0-RC3-SNAPSHOT-jar-with-dependencies.jar \
    assembly/msd-metdisease-request-compound-mesh.jar -d ./rdf
```