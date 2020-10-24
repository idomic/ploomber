import os
from datetime import timedelta, datetime
import numpy as np
import pandas as pd
from pathlib import Path
import pytest
import yaml
from conftest import _path_to_tests, fixture_tmp_dir
import jupytext
import nbformat
import jupyter_client
import getpass

from ploomber.spec.DAGSpec import DAGSpec
from ploomber.util.util import load_dotted_path
from ploomber.tasks import PythonCallable


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'pipeline-sql')
def tmp_pipeline_sql():
    pass


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'pipeline-r')
def tmp_pipeline_r():
    pass


@fixture_tmp_dir(_path_to_tests() / 'assets' /
                 'pipeline-sql-products-in-source')
def tmp_pipeline_sql_products_in_source():
    pass


def to_ipynb(dag_spec):
    for source in ['load.py', 'clean.py', 'plot.py']:
        nb = jupytext.read(source)
        Path(source).unlink()

        k = jupyter_client.kernelspec.get_kernel_spec('python3')

        nb.metadata.kernelspec = {
            "display_name": k.display_name,
            "language": k.language,
            "name": 'python3'
        }

        nbformat.write(nb, source.replace('.py', '.ipynb'))

    for task in dag_spec['tasks']:
        task['source'] = task['source'].replace('.py', '.ipynb')

    return dag_spec


def tasks_list(dag_spec):
    tasks = dag_spec['tasks']

    # we have to pop this, since a list of tasks gets meta default params
    # which extracts both upstream and product from source code
    for t in tasks:
        t.pop('upstream', None)
        t.pop('product', None)

    return tasks


def remove_task_class(dag_spec):
    for task in dag_spec['tasks']:
        del task['class']

    return dag_spec


def extract_upstream(dag_spec):
    dag_spec['meta']['extract_upstream'] = True

    for task in dag_spec['tasks']:
        task.pop('upstream', None)

    return dag_spec


def extract_product(dag_spec):
    dag_spec['meta']['extract_product'] = True

    for task in dag_spec['tasks']:
        task.pop('product', None)

    return dag_spec


def test_validate_top_level_keys():
    with pytest.raises(KeyError):
        DAGSpec({'invalid_key': None})


def test_validate_meta_keys():
    with pytest.raises(KeyError):
        DAGSpec({'tasks': [], 'meta': {'invalid_key': None}})


def test_python_callables_spec(tmp_directory, add_current_to_sys_path):
    Path('test_python_callables_spec.py').write_text("""
def task1(product):
    pass
""")

    spec = DAGSpec({
        'tasks': [
            {
                'source': 'test_python_callables_spec.task1',
                'product': 'some_file.csv'
            },
        ],
        'meta': {
            'extract_product': False,
            'extract_upstream': False
        }
    })

    dag = spec.to_dag()
    assert isinstance(dag['task1'], PythonCallable)


def test_python_callables_with_extract_upstream(tmp_directory):
    spec = DAGSpec({
        'tasks': [
            {
                'source': 'test_pkg.callables.root',
                'product': 'root.csv'
            },
            {
                'source': 'test_pkg.callables.a',
                'product': 'a.csv'
            },
            {
                'source': 'test_pkg.callables.b',
                'product': 'b.csv'
            },
        ],
        'meta': {
            'extract_product': False,
            'extract_upstream': True
        }
    })

    dag = spec.to_dag()

    dag.build()

    assert set(dag) == {'a', 'b', 'root'}
    assert not dag['root'].upstream
    assert set(dag['a'].upstream) == {'root'}
    assert set(dag['b'].upstream) == {'root'}


@pytest.mark.parametrize('processor', [
    to_ipynb, tasks_list, remove_task_class, extract_upstream, extract_product
])
def test_notebook_spec(processor, tmp_nbs):
    Path('output').mkdir()

    with open('pipeline.yaml') as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dag_spec = processor(dag_spec)

    dag = DAGSpec(dag_spec).to_dag()
    dag.build()


def test_notebook_spec_nested(tmp_nbs_nested):
    Path('output').mkdir()
    dag = DAGSpec('pipeline.yaml').to_dag()
    dag.build()


def test_loads_env_if_exists(tmp_nbs):
    Path('env.yaml').write_text("{'a': 1}")
    spec = DAGSpec('pipeline.yaml')
    assert spec.env == {'a': 1}


def test_does_not_load_env_if_loading_from_dict(tmp_nbs):
    Path('env.yaml').write_text("{'a': 1}")

    with open('pipeline.yaml') as f:
        d = yaml.safe_load(f)

    spec = DAGSpec(d)
    assert spec.env is None


def test_notebook_spec_w_location(tmp_nbs, add_current_to_sys_path):

    Path('output').mkdir()

    with open('pipeline-w-location.yaml') as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dag = DAGSpec(dag_spec).to_dag()
    dag.build()


@pytest.mark.parametrize(
    'chdir, dir_',
    [
        # test with the current directory
        ['.', '.'],
        # and one level up
        ['..', 'content'],
    ])
def test_spec_from_directory(tmp_nbs_no_yaml, chdir, dir_):
    os.chdir(chdir)

    Path('output').mkdir()

    dag = DAGSpec.from_directory(dir_).to_dag()
    dag.build()

    assert list(dag) == ['load', 'clean', 'plot']


def _random_date_from(date, max_days, n):
    return [
        date + timedelta(days=int(days))
        for days in np.random.randint(0, max_days, n)
    ]


def test_postgres_sql_spec(tmp_pipeline_sql, pg_client_and_schema,
                           add_current_to_sys_path):
    with open('pipeline-postgres.yaml') as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dates = _random_date_from(datetime(2016, 1, 1), 365, 100)
    df = pd.DataFrame({
        'customer_id': np.random.randint(0, 5, 100),
        'value': np.random.rand(100),
        'purchase_date': dates
    })
    loader = load_dotted_path(dag_spec['clients']['SQLScript'])
    client = loader()
    df.to_sql('sales', client.engine, if_exists='replace')
    client.engine.dispose()

    dag = DAGSpec(dag_spec).to_dag()

    # FIXME: this does no show the custom Upstream key missing error
    dag.build()

    assert not dag['load.sql'].upstream
    assert list(dag['filter.sql'].upstream.keys()) == ['load.sql']
    assert list(dag['transform.sql'].upstream.keys()) == ['filter.sql']


def test_sql_spec_w_products_in_source(tmp_pipeline_sql_products_in_source,
                                       add_current_to_sys_path):
    with open('pipeline.yaml') as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dates = _random_date_from(datetime(2016, 1, 1), 365, 100)
    df = pd.DataFrame({
        'customer_id': np.random.randint(0, 5, 100),
        'value': np.random.rand(100),
        'purchase_date': dates
    })
    loader = load_dotted_path(dag_spec['clients']['SQLScript'])
    client = loader()
    df.to_sql('sales', client.engine, if_exists='replace')
    client.engine.dispose()

    dag = DAGSpec(dag_spec).to_dag()
    dag.build()


@pytest.mark.parametrize('spec',
                         ['pipeline-sqlite.yaml', 'pipeline-sqlrelation.yaml'])
def test_sqlite_sql_spec(spec, tmp_pipeline_sql, add_current_to_sys_path):
    with open(spec) as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dates = _random_date_from(datetime(2016, 1, 1), 365, 100)
    df = pd.DataFrame({
        'customer_id': np.random.randint(0, 5, 100),
        'value': np.random.rand(100),
        'purchase_date': dates
    })
    loader = load_dotted_path(dag_spec['clients']['SQLScript'])
    client = loader()
    df.to_sql('sales', client.engine)
    client.engine.dispose()

    dag = DAGSpec(dag_spec).to_dag()

    # FIXME: this does no show the custom Upstream key missing error
    dag.build()

    assert not dag['load.sql'].upstream
    assert list(dag['filter.sql'].upstream.keys()) == ['load.sql']
    assert list(dag['transform.sql'].upstream.keys()) == ['filter.sql']


def test_mixed_db_sql_spec(tmp_pipeline_sql, add_current_to_sys_path,
                           pg_client_and_schema):
    with open('pipeline-multiple-dbs.yaml') as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dates = _random_date_from(datetime(2016, 1, 1), 365, 100)
    df = pd.DataFrame({
        'customer_id': np.random.randint(0, 5, 100),
        'value': np.random.rand(100),
        'purchase_date': dates
    })
    # make sales data for pg and sqlite
    loader = load_dotted_path(dag_spec['clients']['PostgresRelation'])
    client = loader()
    df.to_sql('sales', client.engine, if_exists='replace')
    client.engine.dispose()

    # make sales data for pg and sqlite
    loader = load_dotted_path(dag_spec['clients']['SQLiteRelation'])
    client = loader()
    df.to_sql('sales', client.engine)
    client.engine.dispose()

    dag = DAGSpec(dag_spec).to_dag()

    # FIXME: this does no show the custom Upstream key missing error
    dag.build()


def test_pipeline_r(tmp_pipeline_r):
    Path('output').mkdir()

    with open('pipeline.yaml') as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dag = DAGSpec(dag_spec).to_dag()
    dag.build()


@pytest.mark.parametrize('raw', [[{
    'source': 'load.py'
}], {
    'tasks': [{
        'source': 'load.py'
    }]
}, {
    'meta': {},
    'tasks': []
}])
def test_meta_defaults(raw):
    spec = DAGSpec(raw)
    meta = spec['meta']
    assert meta['extract_upstream']
    assert meta['extract_product']
    assert not meta['product_relative_to_source']
    assert not meta['jupyter_hot_reload']


@pytest.mark.parametrize('name, value', [
    ['extract_upstream', False],
    ['extract_product', False],
    ['product_relative_to_source', True],
    ['jupyter_hot_reload', True],
])
def test_changing_defaults(name, value):
    spec = DAGSpec({'meta': {name: value}, 'tasks': []})
    assert spec['meta'][name] is value


@pytest.mark.parametrize('save', [True, False])
def test_expand_env(save, tmp_directory):
    env = {'sample': True, 'user': '{{user}}'}

    if save:
        with open('env.yaml', 'w') as f:
            yaml.dump(env, f)
        env = 'env.yaml'

    spec = DAGSpec(
        {
            'tasks': [{
                'source': 'plot.py',
                'params': {
                    'sample': '{{sample}}',
                    'user': '{{user}}'
                }
            }]
        },
        env=env)

    assert spec['tasks'][0]['params']['sample'] is True
    assert spec['tasks'][0]['params']['user'] == getpass.getuser()


@pytest.mark.parametrize('method, kwargs', [
    [None, dict(data='pipeline.yaml')],
    ['auto_load', dict(to_dag=False)],
])
def test_passing_env_in_class_methods(method, kwargs, tmp_directory):

    spec_dict = {
        'tasks': [{
            'source': 'plot.py',
            'params': {
                'some_param': '{{key}}',
            }
        }]
    }

    with open('pipeline.yaml', 'w') as f:
        yaml.dump(spec_dict, f)

    if method:
        callable_ = getattr(DAGSpec, method)
    else:
        callable_ = DAGSpec

    spec = callable_(**kwargs, env={'key': 'value'})

    # auto_load returns a tuple
    if isinstance(spec, tuple):
        spec = spec[0]

    assert spec['tasks'][0]['params']['some_param'] == 'value'


def test_infer_dependencies_sql(tmp_pipeline_sql, add_current_to_sys_path):
    expected = {
        'filter.sql': {'load.sql'},
        'transform.sql': {'filter.sql'},
        'load.sql': set()
    }

    with open('pipeline-postgres.yaml') as f:
        d = yaml.load(f)

    d['meta']['extract_upstream'] = True

    for t in d['tasks']:
        t.pop('upstream', None)

    dag = DAGSpec(d).to_dag()

    deps = {name: set(task.upstream) for name, task in dag.items()}
    assert deps == expected


def test_extract_variables_from_notebooks(tmp_nbs):
    with open('pipeline.yaml') as f:
        d = yaml.load(f)

    d['meta']['extract_upstream'] = True
    d['meta']['extract_product'] = True

    for t in d['tasks']:
        t.pop('upstream', None)
        t.pop('product', None)

    dag = DAGSpec(d).to_dag()
    deps = {name: set(task.upstream) for name, task in dag.items()}
    prods = {name: task.product for name, task in dag.items()}

    expected_deps = {'clean': {'load'}, 'plot': {'clean'}, 'load': set()}

    # expected_prod = {
    #     'clean.py': {
    #         'data': 'output/clean.csv',
    #         'nb': 'output/clean.ipynb'
    #     },
    #     'load.py': {
    #         'data': 'output/data.csv',
    #         'nb': 'output/load.ipynb'
    #     },
    #     'plot.py': 'output/plot.ipynb'
    # }

    assert deps == expected_deps
    # assert prods == expected_prod


def test_source_loader(monkeypatch, tmp_directory):
    monkeypatch.syspath_prepend(tmp_directory)

    spec = DAGSpec({
        'meta': {
            'source_loader': {
                'path': 'templates',
                'module': 'test_pkg'
            },
            'extract_product': False,
            'extract_upstream': False,
        },
        'tasks': [{
            'source': 'create-table.sql',
            'product': ['some_table', 'table'],
            'client': 'db.get_client'
        }]
    })

    Path('db.py').write_text("""
from ploomber.clients import SQLAlchemyClient

def get_client():
    return SQLAlchemyClient('sqlite://')
""")

    # check source loader is working correctly with a template that has a macro
    loader = spec['meta']['source_loader']
    template = loader['create-table.sql']

    expected = ('\nDROP TABLE IF EXISTS some_table;\nCREATE TABLE '
                'some_table AS\nSELECT * FROM table')
    assert template.render({'product': 'some_table'}) == expected

    # test the task source is correctly resolved when converted to a dag
    dag = spec.to_dag()
    dag.render()

    assert str(dag['create-table.sql'].source) == expected


def test_spec_with_functions(backup_spec_with_functions):
    """
    Check we can create pipeline where the task is a function defined in a
    local file
    """
    spec = DAGSpec('pipeline.yaml')
    spec.to_dag().build()
