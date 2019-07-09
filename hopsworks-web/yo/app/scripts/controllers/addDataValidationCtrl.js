'use strict';
angular.module('hopsWorksApp')
    .controller('AddDataValidationCtrl', ['$uibModalInstance', 'features',
        function ($uibModalInstance, features) {
            self = this;
            self.features = features;

            self.columnsModes = {
                NO_COLUMNS: 0,
                SINGLE_COLUMN: 1,
                MULTI_COLUMNS: 2
            }

            self.predicateType = {
                BOUNDARY: 0
            }

            var Predicate = function(name, predicateType, columnsSelectionMode) {
                this.name = name;
                this.predicateType = predicateType;
                this.columnsSelectionMode = columnsSelectionMode;
                this.features = [];
                this.hint = "";
            }

            Predicate.prototype.constructPredicate = function () {
                var features_names = [];
                if (this.columnsSelectionMode == self.columnsModes.NO_COLUMNS) {
                    features_names.push('*');
                } else {
                    for (var i = 0; i < this.features.length; i++) {
                        features_names.push(this.features[i].name)
                    }
                }
                var args = {
                    hint: this.hint
                }
                if (this.predicateType == self.predicateType.BOUNDARY) {
                    args.min = this.min;
                    args.max = this.max;
                }

                var predicate = {
                    feature: features_names,
                    predicate: this.name,
                    arguments: args
                }
                return predicate;
            };

            var hasSize = new Predicate("hasSize", self.predicateType.BOUNDARY,
                self.columnsModes.NO_COLUMNS);
            var hasCompleteness = new Predicate("hasCompleteness",
                self.predicateType.BOUNDARY, self.columnsModes.MULTI_COLUMNS);

            self.valid_predicates = [hasSize, hasCompleteness]

            self.selected_predicate;
        
            self.feature_selected = function () {
                console.log("Feature selected: " + self.selected_feature)
            }

            self.has_predicate_been_selected = function () {
                if (self.selected_predicate) {
                    return true
                }
                return false
            }

            self.addNewPredicate = function () {
                if (self.selected_predicate) {
                    var predicate = self.selected_predicate.constructPredicate();
                    $uibModalInstance.close(predicate);
                } else {
                    $uibModalInstance.dismiss('cancel');
                }
                
            }

            /**
             * Closes the modal
             */
            self.close = function () {
                $uibModalInstance.dismiss('cancel');
            };
        }
    ]);