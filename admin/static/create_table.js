/**
 * @fileoverview This file defines controller for creating a table.
 */

app.controller("CreateTableController",
  [
    '$scope', '$http', '$location', '$rootScope',
    function($scope, $http, $location, $rootScope) {
      $scope.fields = []
      $scope.table_name = "";

      $scope.field_types = ["int64", "double", "string"];

      $scope.addField = function() {
        if ($scope.field_name != undefined && $scope.field_type != undefined) {
          var field = {};
          field.name = $scope.field_name;
          field.field_type = $scope.field_type;

          $scope.fields.push(field);

          $scope.field_name = null;
          $scope.field_type = null;
        }
      };

      $scope.removeField = function (index_to_remove) {
        var new_fields = []
        $scope.fields.forEach(function (value, index) {
          if (index != index_to_remove) {
            new_fields.push(value);
          }
        });
        $scope.fields = new_fields;
      };

      $scope.checkNameValid = function(name) {
        return true;
      }

      $scope.checkFieldTypeValid = function(field_type) {
        return true;
      }

      $scope.checkTableNameValid = function(table_name) {
        return true;
      }

      $scope.cancelCreateTable = function () {
        $location.path("table_list");
      }

      $scope.createTable = function () {
        var err = "";

        if (!$scope.fields || $scope.fields.length == 0) {
          err = "Can not create empty table";
          return;
        }

        if (!$scope.checkTableNameValid($scope.table_name)) {
          err = "Invalid name for the table";
          return;
        }

        $http.post('/api/' + $rootScope.db.name + "/create",
          { table_name: $scope.table_name, fields: $scope.fields },
          { headers: { 'Content-Type': "application/json" } })
        .then(function(resp) {
          $location.path("table_list");
        }, function(err) {
          alert("FAIL: Could not connect.\n" + err);
        });

        $scope.fields = [];
      };
    }
  ]
);