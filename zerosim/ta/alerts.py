import urllib2

class Alerts(object):
    QUANDL_CBOE_SKEW = 'http://www.cboe.com/publish/ScheduledTask/MktData/datahouse/Skewdailyprices.csv'
    QUANDL_CBOE_PCRATIO = 'http://www.cboe.com/publish/ScheduledTask/MktData/datahouse/totalpc.csv'

    def return_remote(self, file_url):
        remote_file = urllib2.urlopen(file_url)
        return remote_file.read().splitlines()

    def skew_alert(self, level):
        skew_element = self.return_remote(self.QUANDL_CBOE_SKEW)
        print 'Skew: ' + skew_element[-1].split()[-1]

    def pcratio_alert(self, level):
        pcratio_element = self.return_remote(self.QUANDL_CBOE_PCRATIO)
        print 'PC Ratio: ' + pcratio_element[-1].split()[-1]


if __name__ == '__main__':

    alert = Alerts()

    alert.pcratio_alert(1)
    alert.skew_alert(1)