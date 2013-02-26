"""
Download, process, and analyze stock data from Yahoo finance.

(c) 2012 Jack Peterson (jack@tinybike.net)
"""

from __future__ import division
import pylab as p
import MySQLdb as sql
import urllib as u
import sys
import math
from collections import Counter

class Stock:
	def __init__(self, symbol):
		self.data = []
		self.date = []
		self.openPrice = []
		self.highPrice = []
		self.lowPrice = []
		self.closePrice = []
		self.volume = []
		self.adjClosePrice = []
		self.count = 0
		self.symbol = symbol.upper()
	
	def importTimeSeries(self):
		self.url = 'http://ichart.finance.yahoo.com/table.csv?s=%s&d=9&e=7&f=2012&g=d&a=0&b=2&c=1950&ignore=.csv' % self.symbol
		self.f = u.urlopen(self.url)
		for self.row in self.f:
			self.data.append(self.row.strip().split(','))
			if self.count > 0:
				self.date.append(self.data[self.count][0])
				self.openPrice.append(float(self.data[self.count][1]))
				self.highPrice.append(float(self.data[self.count][2]))
				self.lowPrice.append(float(self.data[self.count][3]))
				self.closePrice.append(float(self.data[self.count][4]))
				self.volume.append(int(self.data[self.count][5]))
				self.adjClosePrice.append(float(self.data[self.count][6]))
			self.count += 1
			if len(self.data[0]) == 1:
				print 'Error, data not downloaded for %s.' % self.symbol
				return 0
		print 'Downloaded %s.' % self.symbol
		# Reverse lists so oldest date is first
		self.date.reverse()
		self.openPrice.reverse()
		self.highPrice.reverse()
		self.lowPrice.reverse()
		self.closePrice.reverse()
		self.volume.reverse()
		self.adjClosePrice.reverse()
		# Insert data into MySQL DB table
		self.createStockTable()
		return 1
	
	def createStockTable(self):
		# Convert '.' to '_' for DB
		self.symbolAsList = list(self.symbol.strip())
		self.uSymbol = ['_' if x == '.' else x for x in self.symbolAsList]
		self.uSymbol = ''.join(str(x) for x in self.uSymbol)
		self.uSymbol = self.uSymbol + '_daily'
		# Connect to stock DB
		self.db = sql.connect(host = 'localhost', user = 'username', passwd = 'password',  db = 'stock')
		self.cursor = self.db.cursor()
		# Check to see if this symbol has a table already
		self.cursor.execute('show tables;')
		self.showTables = self.cursor.fetchall()
		self.tableList = []
		for row in self.showTables:
			self.tableList.append(row[0].upper())
		if self.uSymbol.upper() in self.tableList:
			print 'Table for %s already exists.' % self.symbol
		else:
			# If not, then create new table
			self.cursor.execute("""CREATE TABLE IF NOT EXISTS %s
				(trading_date DATE,
				open DOUBLE(16,2),
				high DOUBLE(16,2),
				low DOUBLE(16,2),
				close DOUBLE(16,2),
				adj_close DOUBLE(16,2),
				volume INT,
				PRIMARY KEY(trading_date));""" % self.uSymbol)
			for i in range(self.count - 1):
				self.cursor.execute('INSERT INTO %s (trading_date, open, high, low, close, adj_close, volume) VALUES (\'%s\', %f, %f, %f, %f, %f, %f);' % (self.uSymbol, self.date[i], self.openPrice[i], self.highPrice[i], self.lowPrice[i], self.closePrice[i], self.adjClosePrice[i], self.volume[i]))
		self.cursor.close()
		self.db.commit()
		self.db.close()
		
	def getTimeSeries(self):
		# Convert '.' to '_' for DB
		self.symbolAsList = list(self.symbol.strip())
		self.uSymbol = ['_' if x == '.' else x for x in self.symbolAsList]
		self.uSymbol = ''.join(str(x) for x in self.uSymbol)
		self.uSymbol = self.uSymbol + '_daily'
		# Connect to stock DB
		self.db = sql.connect(host = 'localhost', user = 'username', passwd = 'password', db = 'stock')
		self.cursor = self.db.cursor()
		# Check to see if this symbol has a table already
		self.cursor.execute('show tables;')
		self.showTables = self.cursor.fetchall()
		self.tableList = []
		for row in self.showTables:
			self.tableList.append(row[0].upper())
		if self.uSymbol.upper() in self.tableList:
			# Get data from table
			self.cursor.execute('SELECT * FROM %s;' % self.uSymbol)
			self.queryResult = self.cursor.fetchall()
			self.date = []
			self.openPrice = []
			self.highPrice = []
			self.lowPrice = []
			self.closePrice = []
			self.volume = []
			self.adjClosePrice = []
			for row in self.queryResult:
				self.date.append(row[0])
				self.openPrice.append(row[1])
				self.highPrice.append(row[2])
				self.lowPrice.append(row[3])
				self.closePrice.append(row[4])
				self.adjClosePrice.append(row[5])
				self.volume.append(row[6])
		else:
			print 'Error, table for %s does not exist.' % self.symbol
		self.cursor.close()
		self.db.close()
		
	def calcReturns(self):
		self.simpleReturn = []
		self.logReturn = []
		for i in range(1, len(self.closePrice)):
			self.simpleReturn.append((self.closePrice[i] - self.closePrice[i-1]) / self.closePrice[i])
			self.logReturn.append(math.log(self.closePrice[i]) - math.log(self.closePrice[i-1]))
	
	def calcStats(self):
		numReturns = len(self.logReturn)
		self.mean_logReturn = sum(self.logReturn) / numReturns
		central_logReturn = [i - self.mean_logReturn for i in self.logReturn]
		self.var_logReturn = sum([i ** 2 for i in central_logReturn]) / (numReturns - 1)
		self.skew_logReturn = sum([i ** 3 for i in central_logReturn]) / self.var_logReturn ** 1.5
		self.kurt_logReturn = sum([i ** 4 for i in central_logReturn]) / self.var_logReturn ** 2 - 3
		print self.mean_logReturn
		print self.var_logReturn
		print self.skew_logReturn
		print self.kurt_logReturn
		roundLogReturn = [round(i * 100) / 100 for i in self.logReturn]
		self.counts = Counter(roundLogReturn)
		
	def plotHist(self):
		p.figure()
		p.semilogy(self.counts.keys(), self.counts.values(), '.')
		p.xlabel('Log-return')
		p.ylabel('Count')
		p.title(self.symbol)
		p.show()
	
	def autoCorr(self, timeSeries):
		self.N = len(timeSeries)
		self.nfft = int(2 ** math.ceil(math.log(abs(self.N),2)))
		self.ACF = p.ifft(p.fft(timeSeries,self.nfft) * p.conjugate(p.fft(timeSeries,self.nfft)))
		self.ACF = list(p.real(self.ACF[:int(math.ceil((self.nfft+1)/2.0))]))
		self.plotAutoCorr()
		
	def plotAutoCorr(self):
		p.figure()
		p.plot(self.ACF)
		p.xlabel('Delay')
		p.ylabel('ACF')
		p.title(self.symbol)
		p.show()
	
	def plotTimeSeries(self, timeSeries):
		# Open, high, low, close, adjusted closing prices, or volume time series
		if timeSeries.lower() == 'open':
			self.showPlot(self.openPrice, 'Open price')
		elif timeSeries.lower() == 'high':
			self.showPlot(self.highPrice, 'High price')
		elif timeSeries.lower() == 'low':
			self.showPlot(self.lowPrice, 'Low price')
		elif timeSeries.lower() == 'close':
			self.showPlot(self.closePrice, 'Closing price')
		elif timeSeries[:3].lower() == 'adj':
			self.showPlot(self.adjClosePrice, 'Adjusted closing price')
		elif timeSeries.lower() == 'volume':
			self.showPlot(self.volume, 'Volume')
		elif timeSeries.lower() == 'simple':
			self.showPlot(self.simpleReturn, 'Simple return')
		elif timeSeries.lower() == 'log':
			self.showPlot(self.logReturn, 'Log-return')
		else:
			print 'Time series not found.'
			
	def showPlot(self, timeSeries, timeSeriesName):
		p.figure()
		p.plot(timeSeries)
		p.xlabel('Day (from %s to %s)' % (self.date[0], self.date[len(self.date) - 1]))
		p.ylabel('%s' % timeSeriesName)
		p.title(self.symbol)
		p.show()
	
def writeCSV(x, y):
	f = open('/Users/jack/Documents/Dill/stocks/stock_hist.csv', 'w')
	sum_y = sum(y)
	y = [i/sum_y for i in y]
	print y
	for i in xrange(len(x)):
		f.write(str(x[i]) + ',' + str(y[i]) + '\n')
	f.close()

def importExchangeData(path):
	symbolFileName = path
	symbolFile = open(symbolFileName, 'r')
	for symbol in symbolFile:
		s = Stock(symbol.strip())
		s.importTimeSeries()
		
if __name__ == "__main__":
	if sys.argv[1] == 'import':
		NYSE = 'NYSE.csv'
		NASDAQ = 'NASDAQ.csv'
		importExchangeData(NYSE)
		importExchangeData(NASDAQ)
	elif sys.argv[1] == 'tsplot':
		symbol = raw_input('Ticker symbol: ')
		series = raw_input('Open/high/low/close/adjusted/volume: ')
		s = Stock(symbol)
		s.getTimeSeries()
		s.plotTimeSeries(series)
	elif sys.argv[1] == 'analyze':
		symbol = raw_input('Ticker symbol: ')
		s = Stock(symbol)
		s.getTimeSeries()
		s.calcReturns()
		#s.plotTimeSeries('simple')
		#s.plotTimeSeries('log')
		s.calcStats()
		s.plotHist()
		writeCSV(s.counts.keys(), s.counts.values())
		#s.autoCorr(s.logReturn)
		
