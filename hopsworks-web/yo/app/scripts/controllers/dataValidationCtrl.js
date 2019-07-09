'use strict';

angular.module('hopsWorksApp')
    .controller('DataValidationCtrl', ['$scope', '$routeParams', 'ModalService',
        function($scope, $routeParams, ModalService) {

            self = this

            self.showCreateNewDataValidationPage = false;

            self.predicates = []
            self.featureGroupName = $routeParams.featureGroupName
            console.log("Feature group name: " + self.featureGroupName)

            self.toggleNewDataValidationPage = function () {
              self.showCreateNewDataValidationPage = !self.showCreateNewDataValidationPage;
            }

            self.openPredicatesEditor = function () {
                
                var feature_info = self.getFeatureGroupInfo();
                var features = feature_info['features']

                var thisthis = self
                ModalService.addDataValidationPredicate('lg', features).then(
                    function (predicate) {
                        console.log("Success from Modal " + predicate)
                        thisthis.predicates.push(predicate);
                        self = thisthis
                    }, function (error) {
                        console.log("Error from modal " + error);
                        self = thisthis
                    }
                );
            };

            self.finishPredicates = function () {
              console.log("Finished adding predicates");

              var constraints = [];
              for (var i = 0; i < self.predicates.length; i++) {
                var constraint = {
                  name: self.predicates[i].predicate,
                  hint: self.predicates[i].arguments.hint
                }
                if (self.predicates[i].arguments.min) {
                  constraint.min = self.predicates[i].arguments.min;
                }
                if (self.predicates[i].arguments.max) {
                  constraint.max = self.predicates[i].arguments.max;
                }
                constraint.columns = self.predicates[i].feature;
                constraints.push(constraint)
              }

              var constraintGroups = [];
              var constraintGroup = {
                level: "info",
                description: "some description",
                constraints: constraints
              }
              constraintGroups.push(constraintGroup)
              var container = {
                constraintGroups: constraintGroups
              }

              var containerJSON = JSON.stringify(container);
              var escaped = containerJSON.replace(/"/g, '\\"');
              console.log("cg: " + escaped);
              self.toggleNewDataValidationPage();

            }

            self.feature_group_info = {
                "type": "featuregroupDTO",
                "features": [
                  {
                    "name": "average_attendance",
                    "type": "float",
                    "description": "-",
                    "primary": false,
                    "partition": false
                  },
                  {
                    "name": "sum_attendance",
                    "type": "float",
                    "description": "-",
                    "primary": false,
                    "partition": false
                  },
                  {
                    "name": "team_id",
                    "type": "int",
                    "description": "-",
                    "primary": true,
                    "partition": false
                  }
                ],
                "featurestoreId": 12,
                "featurestoreName": "demo_featurestore_admin000_featurestore",
                "id": 141,
                "inodeId": 113084,
                "jobId": 42,
                "jobName": "featurestore_tour_job-Copy1",
                "jobStatus": "Succeeded",
                "lastComputed": "2019-07-03T16:12:55Z",
                "location": "hdfs://10.0.2.15:8020/apps/hive/warehouse/demo_featurestore_admin000_featurestore.db/attendances_features_1",
                "name": "attendances_features",
                "version": 1,
                "featuregroupType": "CACHED_FEATURE_GROUP",
                "hdfsStorePaths": [
                  "hdfs://10.0.2.15:8020/apps/hive/warehouse/demo_featurestore_admin000_featurestore.db/attendances_features_1"
                ],
                "hiveTableType": "MANAGED_TABLE",
                "inputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
              }

            self.getFeatureGroupInfo = function () {
                return self.feature_group_info;
            }
        }
    ]);