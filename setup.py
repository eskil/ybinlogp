import setuptools

setuptools.setup(
	name='ybinlogp',
	version='0.1',
	description='MySQL binlog parser',
	ext_modules=[
		setuptools.Extension('ybinlogp', ['ybinlogp.cc'], libraries=['boost_python'])
		]
	)
