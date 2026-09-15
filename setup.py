from setuptools import setup
from TSVZ import version

setup(
    name='TSVZ',
    version=version,  
    py_modules=['TSVZ', 'TSVZ_old', 'TSVZ_new'],
    description='Append-only tabular key–value store (tsvz-spec-v1)',
    author='Yufei Pan',
    author_email='pan@zopyr.us',
    url='https://github.com/yufei-pan/TSVZ',  # URL to the project’s homepage
    entry_points={
        'console_scripts': [
            'TSVZ = TSVZ:__main__',
			'tsvz = TSVZ:__main__',
        ],
    },
    install_requires=[
        'argparse',
    ],
    extras_require={
        'completion': [
            'argcomplete',
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
	long_description=open('README.md').read(),
	long_description_content_type='text/markdown',
	license='GPLv3+',
)
