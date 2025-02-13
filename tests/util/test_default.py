import os
from pathlib import Path

import pytest

from ploomber.util import default
from ploomber.exceptions import DAGSpecNotFound


@pytest.fixture
def pkg_location():
    parent = Path('src', 'package_a')
    parent.mkdir(parents=True)
    pkg_location = (parent / 'pipeline.yaml')
    pkg_location.touch()
    return str(pkg_location)


def test_entry_point_env_var(monkeypatch, tmp_directory, pkg_location):
    monkeypatch.setenv('ENTRY_POINT', 'some.entry.point')
    assert default.entry_point() == 'some.entry.point'


def test_entry_point_pkg_location(tmp_directory, pkg_location):
    assert default.entry_point() == str(pkg_location)


def test_entry_point_pkg_location_and_yaml(tmp_directory, pkg_location):
    Path('pipeline.yaml').touch()
    assert default.entry_point() == 'pipeline.yaml'


def test_entry_point_pkg_location_ignore_egg_info(tmp_directory):
    for pkg in ['package_a.egg-info', 'package_b']:
        parent = Path('src', pkg)
        parent.mkdir(parents=True)
        pkg_location = (parent / 'pipeline.yaml')
        pkg_location.touch()

    assert default.entry_point() == str(
        Path('src', 'package_b', 'pipeline.yaml'))


def test_entry_point_pkg_location_multiple_pkgs(tmp_directory):
    for pkg in ['package_a', 'package_b']:
        parent = Path('src', pkg)
        parent.mkdir(parents=True)
        pkg_location = (parent / 'pipeline.yaml')
        pkg_location.touch()

    assert default.entry_point() == str(
        Path('src', 'package_a', 'pipeline.yaml'))


def test_entry_point():
    assert default.entry_point() == 'pipeline.yaml'


def test_entry_point_in_parent_folder(tmp_directory):
    Path('dir').mkdir()
    Path('pipeline.yaml').touch()
    os.chdir('dir')
    assert default.entry_point() == str(Path('..', 'pipeline.yaml'))


def test_entry_point_in_src_while_in_sibling_folder(tmp_directory):
    Path('setup.py').touch()
    pkg = Path('src', 'package')
    pkg.mkdir(parents=True)
    (pkg / 'pipeline.yaml').touch()
    Path('tests').mkdir()
    os.chdir('tests')
    assert default.entry_point() == str(
        Path('..', 'src', 'package', 'pipeline.yaml'))


def test_path_to_env_local(tmp_directory):
    Path('env.yaml').touch()

    Path('dir').mkdir()
    Path('dir', 'env.yaml').touch()

    assert default.path_to_env('dir') == str(Path('env.yaml').resolve())


def test_path_to_parent_sibling(tmp_directory):
    Path('dir').mkdir()
    Path('dir', 'env.yaml').touch()

    assert default.path_to_env('dir') == str(Path('dir', 'env.yaml').resolve())


@pytest.mark.parametrize('arg', ['dir', None])
def test_path_to_env_none(tmp_directory, arg):
    Path('dir').mkdir()

    assert default.path_to_env(arg) is None


@pytest.mark.parametrize(
    'to_create, to_move',
    [
        [
            ['environment.yml'],
            '.',
        ],
        [
            ['requirements.txt'],
            '.',
        ],
        [
            ['setup.py'],
            '.',
        ],
        [
            ['setup.py', 'subdir/'],
            'subdir',
        ],
        [
            # environment.yml has higher priority than setup.py
            ['environment.yml', 'package/setup.py', 'package/nested/'],
            'package/nested/',
        ],
        [
            # requirements.txt has higher priority than setup.py
            ['requirements.txt', 'package/setup.py', 'package/nested/'],
            'package/nested/',
        ],
    ])
def test_find_root_recursively(tmp_directory, to_create, to_move):
    expected = Path().resolve()

    for f in to_create:

        Path(f).parent.mkdir(exist_ok=True, parents=True)

        if f.endswith('/'):
            Path(f).mkdir()
        else:
            Path(f).touch()

    os.chdir(to_move)

    assert default.find_root_recursively() == expected


def test_raise_if_no_project_root(tmp_directory):
    with pytest.raises(ValueError) as excinfo:
        default.find_root_recursively(raise_=True)

    expected = "Could not determine project's root directory"
    assert expected in str(excinfo.value)


@pytest.mark.parametrize('to_create, to_move', [
    [
        ['environment.yml', 'src/my_package/pipeline.yaml'],
        '.',
    ],
])
def test_find_package_name(tmp_directory, to_create, to_move):
    for f in to_create:

        Path(f).parent.mkdir(exist_ok=True, parents=True)

        if f.endswith('/'):
            Path(f).mkdir()
        else:
            Path(f).touch()

    os.chdir(to_move)

    assert default.find_package_name() == 'my_package'


def test_error_if_no_package(tmp_directory):
    Path('environment.yml').touch()

    with pytest.raises(ValueError) as excinfo:
        default.find_package_name()

    expected = "Could not find a valid package"
    assert expected in str(excinfo.value)


@pytest.mark.parametrize('filename, name', [
    ['pipeline.yaml', None],
    ['pipeline.serve.yaml', 'serve'],
    [Path('src', 'my_pkg', 'pipeline.yaml'), None],
    [Path('src', 'my_pkg', 'pipeline.serve.yaml'), 'serve'],
])
def test_entry_point_relative(tmp_directory, filename, name):
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    Path(filename).touch()

    assert default.entry_point_relative(name=name) == str(filename)


def test_entry_point_relative_error_if_both_exist(tmp_directory):
    Path('pipeline.yaml').touch()
    dir_ = Path('src', 'some_pkg')
    dir_.mkdir(parents=True)
    (dir_ / 'pipeline.yaml').touch()

    with pytest.raises(ValueError):
        default.entry_point_relative()


def test_entry_point_relative_error_if_doesnt_exist(tmp_directory):
    with pytest.raises(DAGSpecNotFound):
        default.entry_point_relative()
