from setuptools import setup


setup(
        name='wcpan.acd',
        version='1.2.0',
        author='Wei-Cheng Pan',
        author_email='legnaleurc@gmail.com',
        description='An Amazon Cloud Drive API tool.',
        url='https://github.com/legnaleurc/wcpan.acd',
        packages=[
            'wcpan.acd',
        ],
        install_requires=[
            'acdcli',
            'tornado',
            'wcpan.logger',
            'wcpan.worker',
        ],
        classifiers=[
            'Programming Language :: Python :: 3 :: Only',
            'Programming Language :: Python :: 3.5',
        ])
