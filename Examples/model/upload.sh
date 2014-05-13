#home  1.1:8b0391d6ef974cf6acd5685db29161b8
#work  1.1:1f4cd590ddf2484299122f0c8079054f

curl -u 1.1:1f4cd590ddf2484299122f0c8079054f -v -F metadata=@metadata.json -F model=@model.csv http://localhost:8080/api/etl/intersections
