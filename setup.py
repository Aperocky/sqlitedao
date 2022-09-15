from setuptools import setup

with open("readme.md", encoding='utf-8') as f:
    readme = f.read()

setup(name='sqlitedao',
      version='0.7.0',
      description='simple DAO builder and abstraction for sqlite',
      author='Rocky Li',
      url='https://github.com/Aperocky/sqlitedao',
      author_email='aperocky@gmail.com',
      license='MIT',
      long_description=readme,
      long_description_content_type='text/markdown',
      packages=['sqlitedao'])
