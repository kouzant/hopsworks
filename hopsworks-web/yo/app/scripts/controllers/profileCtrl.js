/*
 * Changes to this file committed after and not including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
 * This file is part of Hopsworks
 * Copyright (C) 2018, Logical Clocks AB. All rights reserved
 *
 * Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 *
 * Changes to this file committed before and including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
 * Copyright (C) 2013 - 2018, Logical Clocks AB and RISE SICS AB. All rights reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS  OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

'use strict'

angular.module('hopsWorksApp')
        .controller('ProfileCtrl', ['UserService', '$location', '$scope', 'md5', 'growl', '$uibModalInstance','$cookies',
          function (UserService, $location, $scope, md5, growl, $uibModalInstance, $cookies) {

            var self = this;
            self.working = false;
            self.credentialWorking = false;
            self.twoFactorWorking = false;
            self.apiKeysWorking = false;
            self.noPassword = false;
            self.otp = $cookies.get('otp');
            self.emailHash = '';
            self.master = {};
            self.masterTwoFactor = {};
            self.user = {
              firstname: '',
              lastname: '',
              email: '',
              phoneNumber: '',
              twoFactor: ''
            };

            self.loginCredes = {
              oldPassword: '',
              newPassword: '',
              confirmedPassword: ''
            };
            
            self.third_party_api_keys = [];

            self.new_api_key = {
              name: '',
              key: ''
            };

            self.twoFactorAuth = {
              password: '',
              twoFactor: ''
            };

            self.profile = function () {
              UserService.profile().then(
                      function (success) {
                        self.user = success.data;
                        self.emailHash = md5.createHash(self.user.email || '');
                        self.master = angular.copy(self.user);
                        self.twoFactorAuth.twoFactor = self.master.twoFactor;
                        self.user.numRemainingProjects = self.user.maxNumProjects-self.user.numCreatedProjects;
                      },
                      function (error) {
                        self.errorMsg = error.data.errorMsg;
                      });
            };

            self.profile();

            self.updateProfile = function () {
              self.working = true;
              UserService.updateProfile(self.user).then(
                      function (success) {
                        self.working = false;
                        self.user = success.data;
                        self.master = angular.copy(self.user);
                        growl.success("Your profile is now saved.", {title: 'Success', ttl: 5000, referenceId: 1});
                        $scope.profileForm.$setPristine();
                      }, function (error) {
                        self.working = false;
                        self.errorMsg = (typeof error.data.usrMsg !== 'undefined')? error.data.usrMsg : "";
                        growl.error(self.errorMsg, {title: error.data.errorMsg, ttl: 5000, referenceId: 1});
              });
            };

            self.changeLoginCredentials = function (isFormValid) {
              self.credentialWorking = true;
              if (isFormValid) {
                UserService.changeLoginCredentials(self.loginCredes).then(
                        function (success) {
                          self.credentialWorking = false;
                          growl.success("Your password is now updated.", {title: 'Success', ttl: 5000, referenceId: 1});
                        }, function (error) {
                          self.credentialWorking = false;
                          self.errorMsg = (typeof error.data.usrMsg !== 'undefined')? error.data.usrMsg : "";
                          growl.error(self.errorMsg, {title: error.data.errorMsg, ttl: 5000, referenceId: 1});
                });
              }
            };


            self.load_third_party_api_keys = function () {
              self.apiKeysWorking = true;
              UserService.load_third_party_api_keys().then(
                function (success) {
                  self.third_party_api_keys = success.data.items
                  if (!self.third_party_api_keys) {
                    self.third_party_api_keys = []
                  }
                  self.apiKeysWorking = false;
                }, function (error) {
                  self.apiKeysWorking = false;
                  self.errorMsg = (typeof error.data.usrMsg !== 'undefined')? error.data.usrMsg : "";
                  growl.error(self.errorMsg, {title: error.data.errorMsg, ttl: 5000, referenceId: 1});
                }
              );
            }

            self.delete_third_party_api_key = function (key) {
              self.apiKeysWorking = true;
              UserService.delete_third_party_api_key(key.name).then(
                function (success) {
                  self.load_third_party_api_keys();
                  self.apiKeysWorking = false;
                }, function (error) {
                  self.apiKeysWorking = false;
                  self.errorMsg = (typeof error.data.usrMsg !== 'undefined')? error.data.usrMsg : "";
                  growl.error(self.errorMsg, {title: error.data.errorMsg, ttl: 5000, referenceId: 1});
                }
              );
            }

            self.addApiKey = function(isFormValid) {
              self.apiKeysWorking = true;
              if (isFormValid) {
                UserService.add_new_api_key(self.new_api_key).then(
                  function (success) {
                    self.load_third_party_api_keys();
                    self.apiKeysWorking = false;
                    growl.success("Added new API key", {title: 'Success', ttl: 5000, referenceId: 1});
                  }, function (error) {
                    self.apiKeysWorking = false;
                    self.errorMsg = (typeof error.data.usrMsg !== 'undefined')? error.data.usrMsg : "";
                    growl.error(self.errorMsg, {title: error.data.errorMsg, ttl: 5000, referenceId: 1});
                  }
                );
              }
            };

            self.delete_all_third_party_api_keys = function() {
              UserService.delete_all_third_party_api_keys().then(
                function (success) {
                  self.third_party_api_keys = []
                }, function (error) {
                  self.errorMsg = (typeof error.data.usrMsg !== 'undefined')? error.data.usrMsg : "";
                  growl.error(self.errorMsg, {title: error.data.errorMsg, ttl: 5000, referenceId: 1});
                }
              );
            };
            
            self.changeTwoFactor = function() {
              if (self.twoFactorAuth.twoFactor !== self.master.twoFactor) {
                self.twoFactorWorking = true;
                UserService.changeTwoFactor(self.twoFactorAuth).then(
                        function (success) {
                          self.twoFactorWorking = false;
                          self.twoFactorAuth.password = '';
                          if (success.data.QRCode !== undefined) {
                            self.close();
                            $location.path("/qrCode/profile/" + success.data.QRCode);
                          } else if (success.data.successMessage !== undefined) { 
                            self.master.twoFactor = false;
                            growl.success(success.data.successMessage, {title: 'Success', ttl: 5000, referenceId: 1});
                          }
                        }, function (error) {
                          self.twoFactorWorking = false;
                          self.errorMsg = (typeof error.data.usrMsg !== 'undefined')? error.data.usrMsg : "";
                          growl.error(self.errorMsg, {title: error.data.errorMsg, ttl: 5000, referenceId: 1});
                });
              }
            };
            
            self.getQR = function() {
                if (self.twoFactorAuth.password === undefined || 
                    self.twoFactorAuth.password === '') {
                    self.noPassword = true;
                    return;
                }
                self.noPassword = false;
                self.twoFactorWorking = true;
                UserService.getQR(self.twoFactorAuth.password).then(
                        function (success) {
                          self.twoFactorWorking = false;
                          self.twoFactorAuth.password = '';
                          if (success.data.QRCode !== undefined) {
                            self.close();
                            $location.path("/qrCode/profile/" + success.data.QRCode);
                          } 
                        }, function (error) {
                          self.twoFactorWorking = false;
                          self.errorMsg = (typeof error.data.usrMsg !== 'undefined')? error.data.usrMsg : "";
                          growl.error(self.errorMsg, {title: error.data.errorMsg, ttl: 5000, referenceId: 1});
                });
            };
            
            self.externalAccountType = function () {
                return self.user.accountType !== 'M_ACCOUNT_TYPE';
            };

            self.reset = function () {
              self.user = angular.copy(self.master);
              $scope.profileForm.$setPristine();
            };

            self.close = function () {
              $uibModalInstance.dismiss('cancel');
            };

          }]);
