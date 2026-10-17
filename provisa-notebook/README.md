# provisa-notebook

The files Provisa's Data Science area runs in the browser: Pyodide with numpy, pandas, pyarrow,
scikit-learn and matplotlib, and the provisa-client wheel. Provisa's UI server serves them from
this package at `/pyodide`, so a browser needs no CDN or package index.

It is part of every Provisa install: provisa depends on it at its own version, so
`pip install provisa` brings it. It is a package of its own only because PyPI caps a file at
100 MB. The assets are fetched when the package is built (`python -m provisa_notebook.build`),
not when it is installed.
