# provisa-notebook

The files Provisa's Data Science area runs in the browser: Pyodide with numpy, pandas, pyarrow,
scikit-learn and matplotlib, and the provisa-client wheel. Provisa's UI server serves them from
this package at `/pyodide`, so a browser needs no CDN or package index.

Install it with `pip install "provisa[notebook]"`. The assets are fetched when the package is
built (`python -m provisa_notebook.build`), not when it is installed.
