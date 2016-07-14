'use strict';

angular.module('hopsWorksApp')
        .controller('ZeppelinCtrl', ['$scope', '$routeParams','$route',
          'growl', 'ModalService', 'ZeppelinService',
          function ($scope, $routeParams, $route, growl, ModalService, ZeppelinService) {

            var self = this;
            self.interpretersRefreshing = false;
            self.notesRefreshing = false;
            self.interpreters = [];
            self.tutorialNotes = [];
            self.notes = [];
            self.transition = false;
            self.collapse = false;
            $scope.tgState = true;
            self.selectedInterpreter;
            var projectId = $routeParams.projectID;
            var statusMsgs = ['stopped    ', "running    ", 'stopping...', 'restarting...'];

            self.deselect = function () {
              self.selected = null;
              refresh();
              getNotesInProject();
            };

            var getInterpreterStatus = function () {
              self.interpreters = [];
              ZeppelinService.interpreters().then(function (success) {
                for (var k in success.data.body) {
                  self.interpreters.push({interpreter: success.data.body[k], statusMsg: statusMsgs[(success.data.body[k].notRunning ? 0 : 1)]});
                }
              }, function (error) {
                console.log('getInterpreterStatus-->',error);
              });
            };

            var getNotesInProject = function () {
              self.notesRefreshing = true;
              ZeppelinService.notebooks().then(function (success) {
                self.notes = success.data.body;
                self.notesRefreshing = false;
              }, function (error) {
                self.notesRefreshing = false;
                console.log('getNotesInProject-->',error);
              });
            };

            self.display = function (group) {
              if (group.indexOf("spark") > -1)
                return true;
              if (group.indexOf("flink") > -1)
                return true;
//              if (group.indexOf("sh") > -1)
//                return true;
              if (group.indexOf("angular") > -1)
                return true;
              if (group.indexOf("livy") > -1) 
                return true;
              if (group.indexOf("md") > -1) 
                return true;
              return false;
            };

            var refresh = function () {
              self.interpretersRefreshing = true;
              ZeppelinService.interpreters().then(function (success) {
                for (var i in self.interpreters) {
                  var interpreter = success.data.body;
                  if (self.interpreters[i].statusMsg === statusMsgs[0] ||
                          self.interpreters[i].statusMsg === statusMsgs[1]) {
                    self.interpreters[i].interpreter = interpreter[self.interpreters[i].interpreter.name];
                    self.interpreters[i].statusMsg = statusMsgs[(interpreter[self.interpreters[i].interpreter.name].notRunning ? 0 : 1)];
                  }
                }
                self.interpretersRefreshing = false;
              }, function (error) {
                self.interpretersRefreshing = false;
                console.log('refresh-->',error);
              });
            };

            var init = function () {
              self.connectedStatus = ZeppelinService.websocket().isConnected();
              getInterpreterStatus();
              getNotesInProject();
              $scope.tgState = true;
            };

            self.stopInterpreter = function (interpreter) {
              if (!interpreter.interpreter.notRunning) {
                self.transition = true;
                interpreter.statusMsg = statusMsgs[2];
                ZeppelinService.restartInterpreter(interpreter.interpreter.id)
                        .then(function (success) {
                          interpreter.interpreter = success.data.body;
                          interpreter.statusMsg = statusMsgs[(success.data.body.notRunning ? 0 : 1)];
                          self.transition = false;
                        }, function (error) {
                          getInterpreterStatus();
                          self.transition = false;
                          growl.error(error.data.errorMsg, {title: 'Error', ttl: 5000, referenceId: 10});
                        });
              }
            };
            
            self.stopLivySession = function (interpreter, sessionId) {
                self.transition = true;
                interpreter.statusMsg = statusMsgs[2];
                ZeppelinService.stopLivySession(interpreter.interpreter.id, sessionId)
                        .then(function (success) {
                          interpreter.interpreter = success.data.body;
                          interpreter.statusMsg = statusMsgs[(success.data.body.notRunning ? 0 : 1)];
                          self.transition = false;
                        }, function (error) {
                          getInterpreterStatus();
                          self.transition = false;
                          growl.error(error.data.errorMsg, {title: 'Error', ttl: 5000, referenceId: 10});
                        });
            };

            self.refreshInterpreters = function () {
              refresh();
            };

            self.refreshDashboard = function () {
              getNotesInProject();
            };
            self.openNote = function (note) {
              window.open(getLocationBase() + "/zeppelin/#/notebook/" + note.id);
            };

            self.openZeppelin = function () {
              window.open(getLocationBase() + "/zeppelin");
            };

            self.createNewNote = function () {
              var noteName;
              ModalService.noteName('md', '', '', '').then(
                      function (success) {
                        noteName = success.val;
                        ZeppelinService.createNotebook(noteName).then(function (success) {
                          self.notes.push(success.data.body);
                          growl.success("Notebook created successfully.", {title: 'Success', ttl: 5000, referenceId: 10});
                        }, function (error) {
                          growl.error(error.data.errorMsg, {title: 'Error', ttl: 5000, referenceId: 10});
                        });
                      },
                      function (error) {
                      });

            };
            
            self.clearCache = function () {
              ZeppelinService.restart().then( function (success) {
                  //$route.reload();
                }, function (error) {
                  growl.error(error.data.errorMsg, {title: 'Error', ttl: 5000, referenceId: 10});
                  //$route.reload();
                });
            };
            
            self.selectInterpreter = function(interpreter) {
              self.selectedInterpreter = interpreter;
              console.log(interpreter);
            };

            ZeppelinService.websocket().ws.onMessage(function (event) {
              var payload;
              console.log('Receive event << %o', event);
              if (event.data) {
                payload = angular.fromJson(event.data);
              }
              console.log('Receive << %o, %o', payload.op, payload);
              var op = payload.op;
              var data = payload.data;
              if (op === 'NOTE') {
                init();
                console.log('NOTE', data.note);
              } else if (op === 'NOTES_INFO') {
                init();
                console.log('NOTES_INFO', data);
              } else if (op === 'PARAGRAPH_APPEND_OUTPUT') {
                //init();
                getInterpreterStatus();
                console.log('PARAGRAPH_APPEND_OUTPUT', data);
              }
            });
            
            ZeppelinService.websocket().ws.onOpen(function () {
              console.log('Websocket created');
            });

            ZeppelinService.websocket().ws.onError(function (event) {
              console.log('error message: ', event);
              self.connectedStatus = false;
            });

            ZeppelinService.websocket().ws.onClose(function (event) {
              console.log('close message: ', event);
              //close code should be 1012 (service restart) but chrome shows 
              //closed abnormally (1006) this might cause problem when the socket 
              //is closed but not restarted 
              if (event.code === 1012 || event.code === 1006) {
                $route.reload();
              }
              self.connectedStatus = false;
            });  
            
            ZeppelinService.websocket().sendNewEvent({principal: 'anonymous', ticket:'anonymous', op: 'LIST_NOTES'});

          }]);
