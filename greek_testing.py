__author__ = 'achau'
import scipy
import mibian

# BS([underlyingPrice, strikePrice, interestRate, daysToExpiration], \
# volatility=x, callPrice=y, putPrice=z)

c = mibian.BS([2079.65, 2100, 1, 27], callPrice=16.55)
print(c.impliedVolatility)