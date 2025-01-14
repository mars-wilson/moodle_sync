from setuptools import setup, find_packages

setup(
    name='MoodleSync',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
        # list your dependencies here
    ],
    author='Michael Mars Landis',
    author_email='mlandis+moodlesync@warren-wilson.edu',
    description='A module for syncing courses and enrollments to Moodle via API',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/mars-wilson/moodle_sync',  # URL of your project
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.11',
)
