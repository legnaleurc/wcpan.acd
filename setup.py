from setuptools import setup


setup(
        name='wcpan.acd',
        version='1.0.0.dev11',
        author='Wei-Cheng Pan',
        author_email='legnaleurc@gmail.com',
        url='https://github.com/legnaleurc/wcpan.acd',
        packages=[
            'wcpan.acd',
        ],
        install_requires=[
            'acdcli',
            'wcpan.logger',
            'wcpan.worker',
        ],
        classifiers=[
            'Programming Language :: Python :: 3 :: Only',
            'Programming Language :: Python :: 3.5',
        ])
