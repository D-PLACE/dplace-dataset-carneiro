from setuptools import setup


setup(
    name='cldfbench_carneiro',
    py_modules=['cldfbench_carneiro'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'carneiro=cldfbench_carneiro:Dataset',
        ]
    },
    install_requires=[
        'cldfbench',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
