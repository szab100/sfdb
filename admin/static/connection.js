/**
 * @fileoverview This file defines connection controller.
 */
app.controller("ConnectionController",
  [
    '$scope', '$http', '$location', '$rootScope',
    function($scope, $http, $location, $rootScope) {
      var init = function() {
        $scope.db = {
          user: "root",
          password: "",
          host: "localhost",
          port: 27910,
          ttl: 5,
          name: "MAIN" // TODO: for now only 1 DB used
        };

        $scope.ttls = [];
        // TTL in seconds for gRPC connection
        angular.forEach([5, 15, 45], function(ttl) {
          $scope.ttls.push({value: ttl});
        });

        $scope.connected = false;
      };

      init();

      $scope.toggleConnection = function() {
        $scope.connected ? disconnect() : connect();
      };

      function connect () {
        var connString = "".concat(
            $scope.db.user, ":", $scope.db.password, "@",
            $scope.db.host, ":", $scope.db.port, "/",
            $scope.db.name, "?", "ttl=", $scope.db.ttl);

        $http.post('/api/connect', JSON.parse('{ "conn_str": "' + connString + '" }'), {
          headers: { 'Content-Type': "application/json" }
        })
          .then(function(resp) {
            $scope.connected = true;
            $rootScope.db = $scope.db
            $location.path("table_list");
          }, function(err) {
            alert("FAIL: Could not connect.\n" + err);
          });
      }

      function disconnect() {
        $http.post('/api/close', {})
          .then(function (resp) {
            $scope.connected = false;
            $rootScope.db = null
            alert("Closed DB connection.");
          });
      }
    }
  ]
 );