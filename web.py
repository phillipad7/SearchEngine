import cherrypy
import os, os.path
from collections import defaultdict
from procquery import searchQuery



class BC:
    def index(self):
        self.queries = ''
        html = open('search.html', 'r').read()
#        output = html
        return html

    index.exposed = True

    # this is the function triggered by the form action
    def getInput(self, usrin=None):
        # usrin is user input
        # output = 'no' if usrin is None else 'ddd {} !'.format( usrin )
        self.queries = usrin
        result = searchQuery(self.queries)

        # for url, snip in result.items():
        #     print('\nurl : {}'.format(url))
        #     for n, s in enumerate(snip):
        #         print(n, s[:20])


        html = self.modifyOutput(result)
        # raise cherrypy.HTTPRedirect('http://cctv.com.cn')

        return html



    getInput.exposed=True

    # adict{url:[snip], ...}
    def modifyOutput(self, adict):
        # copy first half of html
        f1 = open('result1st.html', 'r')
        firsthalf = f1.read()
        f1.close()
        # write html for the search results
        astr = ''
        for url, snip in adict.items():
            astr += '<br><div class="url"><a href={} onclick="alert("not ye??")" status=301 method="GET")>{}</a></div>'.format(url, url, url)
            for s in snip:
                astr += '<div class ="snip">{}</div>'.format(s)
        astr += '</div></div></body></html>'
        sechalf = open('result.html', 'w+')

        sechalf.write(firsthalf)
        sechalf.write(astr)
        sechalf.close()

        rehtml = open('result.html', 'r').read()
        # return formatted html
        return rehtml


    # modifyOutput.exposed=True
    def rediHTTP():
        print('\n\n\n ~~~~ .... ~~~~ inside rediHTTP ~~~~ .... ~~~~ \n\n\n')
        raise cherrypy.HTTPRedirect('google.com')





if __name__ == '__main__':

    print('path:  ', os.path.abspath(os.getcwd()))

    conf = {
        '/': {
            'tools.sessions.on': True,
            # 'tools.staticdir.root': os.path.abspath(os.getcwd())
            'tools.staticdir.root': 'https://'
            },
        '/static': {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': os.getcwd()
            }
        }
    cherrypy.quickstart(BC(),'/', conf)
    # cherrypy.quickstart(BC(), conf)
    # cherrypy.quickstart(BC())
