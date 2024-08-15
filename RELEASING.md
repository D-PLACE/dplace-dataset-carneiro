# Releasing Carneiro

```shell
cldfbench download cldfbench_carneiro.py
```

```shell
cldfbench makecldf cldfbench_carneiro.py --with-cldfreadme --glottolog-version v5.0
pytest
```

```shell
cldfbench cldfviz.map cldf --pacific-centered --format png --width 20 --output map.png --with-ocean --no-legend
```

```shell
cldferd --format compact.svg cldf > erd.svg
```

```shell
cldfbench readme cldfbench_carneiro.py
cldfbench zenodo --communities dplace cldfbench_carneiro.py
dplace check cldfbench_carneiro.py
```
