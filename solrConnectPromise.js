var http = require('http');

var solrConnectPromise = function(settings){
    this.settings = settings;
};

solrConnectPromise.prototype = {
    constructor:solrConnectPromise,

    // Function to initialize solrConnectPromise Connection Object
    createConnection:function (method, settings, contentLength){
        objConnection = {};
        if (method == "GET") {
            var path = settings.solrCore.concat(settings.solrDataPath);
        } else {
            var path = settings.solrCore.concat(settings.solrUpdatePath);
            objConnection.headers = {
                'Content-Type': 'application/json',
                'Content-Length': contentLength
            };
        }
        objConnection.host = settings.serverAddress;
        objConnection.port = settings.solrPort;
        objConnection.path = path;
        objConnection.method = method;
        objConnection.coreName = settings.solrCore;
        return objConnection;
    },

    // Function to make request to Solr
    postRequest:function(connectionParams, docString, parseResponse){
        callBackPromise = this.returnResponse;
        return new Promise(function(resolve, reject){

            var sendRequest = http.request(connectionParams, function(response){
                response.setEncoding('utf-8');
                var responseString = '';


                response.on('data', function(data){
                    responseString += data;
                });

                response.on('end', function(){
                    resolve(responseString);
                });

                response.on('error', function(err){
                    reject(err);
                })
            });
            sendRequest.write(docString);
            sendRequest.end();
        });
    },

    // Function to build request to update or create new document in solr
    updateCreate:function (docString){
        connectionParams = new this.createConnection('POST', this.settings, docString.length);
        objRes = this.postRequest(connectionParams, docString).then(function(objResponse){
            return new Promise(function(resolve,reject){
                this.callBackPromise(objResponse, 'full_response').then(function(objReturn){
                    if(objReturn.responseHeader.status == 0){
                        resolve(objReturn);
                    } else {
                        reject(objReturn)
                    }
                });
            }.bind(this))
        });
        return new Promise(function(resolve, reject){
            resolve(objRes);
        });
    },

    // Function to delete document in solr by unique document ID
    deletById:function (docID){
        docString = JSON.stringify({"delete": {"id":docID}});
        connectionParams = new this.createConnection('POST', this.settings, docString.length);
        objRes = this.postRequest(connectionParams, docString).then(function(objResponse){
            return new Promise(function(resolve,reject){
                this.callBackPromise(objResponse, 'full_response').then(function(objReturn){
                    if(objReturn.responseHeader.status == 0){
                        resolve(objReturn);
                    } else {
                        reject(objReturn)
                    }
                });
            }.bind(this))
        });
        return new Promise(function(resolve, reject){
            resolve(objRes);
        });
    },

    // Function to delete document in solr by using a query string
    deleteByQuery:function (queryString){
        docString = JSON.stringify('{"query": {' + queryString +'}}');
        connectionParams = new this.createConnection('POST', this.settings, docString.length);
        objRes = this.postRequest(connectionParams, docString).then(function(objResponse){
            return new Promise(function(resolve,reject){
                this.callBackPromise(objResponse, 'full_response').then(function(objReturn){
                    if(objReturn.responseHeader.status == 0){
                        resolve(objReturn);
                    } else {
                        reject(objReturn)
                    }
                });
            }.bind(this))
        });
        return new Promise(function(resolve, reject){
            resolve(objRes);
        });
        this.postRequest(connectionParams, docString, 'full_response');
    },

    // Function to get data from solr
    getData:function (queryString, cursorMark){
        if (typeof cursorMark === 'undefined') {
            queryString = queryString.concat('&cursorMark=*');
        } else {
            queryString = queryString.concat('&cursorMark=', cursorMark);
        }
        connectionParams = new this.createConnection('GET', this.settings, null);
        connectionParams.path = connectionParams.path.concat(queryString);
        objRes = this.postRequest(connectionParams, '').then(function(objResponse){
            return new Promise(function(resolve,reject){
                this.callBackPromise(objResponse, 'data_only').then(function(objReturn){
                    if(objReturn.responseHeader.status == 0){
                        resolve(objReturn);
                    } else {
                        reject(objReturn)
                    }
                });
            }.bind(this))
        });
        return new Promise(function(resolve, reject){
            resolve(objRes);
        });
    },

    // Function to get facets from solr
    getFacets:function (queryString){
        connectionParams = new this.createConnection('GET', this.settings, null);
        connectionParams.path = connectionParams.path.concat(queryString);
        objRes = this.postRequest(connectionParams, '').then(function(objResponse){
            return new Promise(function(resolve,reject){
                this.callBackPromise(objResponse, 'facets_only').then(function(objReturn){
                    if(objReturn.responseHeader.status == 0){
                        resolve(objReturn);
                    } else {
                        reject(objReturn)
                    }
                });
            }.bind(this))
        });
        return new Promise(function(resolve, reject){
            resolve(objRes);
        });
    },

    // Function to get data & facets from solr
    getDataFacets:function (queryString){
        if (typeof cursorMark === 'undefined') {
            queryString = queryString.concat('&cursorMark=*');
        } else {
            queryString = queryString.concat('&cursorMark=', cursorMark);
        }
        connectionParams = new this.createConnection('GET', this.settings, null);
        connectionParams.path = connectionParams.path.concat(queryString);
        objRes = this.postRequest(connectionParams, '').then(function(objResponse){
            return new Promise(function(resolve,reject){
                this.callBackPromise(objResponse, 'data_facets').then(function(objReturn){
                    if(objReturn.responseHeader.status == 0){
                        resolve(objReturn);
                    } else {
                        reject(objReturn)
                    }
                });
            }.bind(this))
        });
        return new Promise(function(resolve, reject){
            resolve(objRes);
        });
    },

    // Function to get record count from solr
    getRecordCount:function (queryString){
        connectionParams = new this.createConnection('GET', this.settings, null);
        connectionParams.path = connectionParams.path.concat(queryString);
        objRes = this.postRequest(connectionParams, '').then(function(objResponse){
            return new Promise(function(resolve,reject){
                this.callBackPromise(objResponse, 'record_count').then(function(objReturn){
                    console.log(objReturn.response);
                    if(objReturn.responseHeader.status == 0){
                        resolve(objReturn.response);
                    } else {
                        reject(objReturn)
                    }
                });
            }.bind(this))
        });
        return new Promise(function(resolve, reject){
            resolve(objRes);
        });
    },

    // Function to get field list from schema
    getFieldList:function (){
        connectionParams = new this.createConnection('GET', this.settings, null);
        connectionParams.path = connectionParams.coreName.concat('/schema');
        objRes = this.postRequest(connectionParams, '').then(function(objResponse){
            return new Promise(function(resolve,reject){
                this.callBackPromise(objResponse, 'fieldList').then(function(objReturn){
                    if(objReturn.responseHeader.status == 0){
                        resolve(objReturn);
                    } else {
                        reject(objReturn)
                    }
                });
            }.bind(this))
        });
        return new Promise(function(resolve, reject){
            resolve(objRes);
        });
    },

    // Function to get leader for solr clusters
    getLeader:function(){
        connectionParams = new this.createConnection('GET', this.settings, null);
        connectionParams.coreName = '/solr';
        connectionParams.path = connectionParams.coreName.concat('/admin/collections?action=OVERSEERSTATUS&wt=json');
        objRes = this.postRequest(connectionParams, '').then(function(objResponse){
            return new Promise(function(resolve,reject){
                this.callBackPromise(objResponse, 'leader').then(function(objReturn){
                    if(objReturn.responseHeader.status == 0){
                        resolve(objReturn);
                    } else {
                        reject(objReturn)
                    }
                });
            }.bind(this))
        });
        return new Promise(function(resolve, reject){
            resolve(objRes);
        });
    },

    // Function to return data from solr back to requesting function
    returnResponse:function (objResponse, parseResponse){
        objResponse = JSON.parse(objResponse);

        return new Promise(function(resolve, reject){

            if (objResponse.responseHeader.status == 400)
            {
                reject(objResponse.responseHeader.error)
            } else if (objResponse.responseHeader.status != 0) {
                error = {"msg": "Unable to complete Solr query.", code: "501"};
            } else {
                // Add cursor mark for pagination to response since it lives outside of response by defualt - this allows us to return just the data when making a data only
                if (typeof objResponse.nextCursorMark !== 'undefined'){
                    objResponse.response.nextCursorMark = objResponse.nextCursorMark
                }

                switch (parseResponse) {

                    case 'data_only':
                        objReturn = {responseHeader:objResponse.responseHeader, response:objResponse.response};
                    break;

                    case 'full_response':
                        objReturn = objResponse;
                    break;

                    case 'facets_only':
                        objReturn = {responseHeader:objResponse.responseHeader, response:objResponse.facet_counts.facet_fields};
                    break;

                    case 'data_facets':
                        objReturn = {responseHeader:objResponse.responseHeader, data:{},facet_counts:{}};
                        objReturn.data = objResponse.response;
                        objReturn.facet_counts = objResponse.facet_counts.facet_fields;
                    break;

                    case 'record_count':
                        objReturn = {responseHeader:objResponse.responseHeader, response:objResponse.response.numFound};
                    break;

                    case 'fieldList':
                        objReturn = {responseHeader:objResponse.responseHeader, fields:objResponse.schema.fields}
                    break;

                    case 'leader':
                        if (objResponse.responseHeader.status == 0){
                            tmpArray = objResponse.leader.split('_');
                            leaderArray = tmpArray[0].split(':');
                            objReturn = {responseHeader:{status:0}, serverAddress:leaderArray[0], solrPort:leaderArray[1]};
                        } else {
                            objReturn = {status:"error", message:objResponse.error.msg};
                        }
                    break;

                    default:
                        objReturn = objResponse;
                    break;
                }
                resolve(objReturn);
            }
        });
    },


}

module.exports = solrConnectPromise;