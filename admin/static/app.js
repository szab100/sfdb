(function() {
  angular.module('sfdb', ['ngMaterial', 'ngMessages']);

  angular.module('sfdb')
      .controller("MainCtrl", ['$scope', '$http', MainCtrl]);

  function MainCtrl($scope, $http) {
    var init = function() {
      $scope.db = {
        user: "root",
        password: "",
        host: "localhost",
        port: 27910,
        ttl: 5,
        name: "MAIN" // TODO: for now only 1 DB used
      };

      $scope.tables = [];

      $scope.table = {
        name: "",
        cols: []
      };

      $scope.request = {
        query: ""
      };

      // response from query, exec
      $scope.data = [];

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

      $http.post('/api/connect', connString, {
        headers: { 'Content-Type': "text/plain" }
      })
        .then(function(resp) {
          $scope.connected = true;
          $scope.showTables();
        }, function(err) {
          alert("FAIL: Could not connect.\n" + err);
        });
    }

    function disconnect() {
      $http.post('/api/close', {})
        .then(function (resp) {
          $scope.connected = false;
          $scope.tables = [];
          $scope.table = {};
          alert("Closed DB connection.");
        });
    }

    $scope.exec = function() {
    };

    $scope.query = function() {
      var url = ['/api', $scope.db.name, $scope.table.name, "query"].join("/");
      $http.post(url, $scope.request.query,
          { headers: { 'Content-Type': "text/plain" }})
        .then(function (resp) {
          $scope.data = resp.data;
        });
    };

    $scope.showTables = function() {
      $http.get('/api/' + $scope.db.name)
        .then(function (resp) {
          $scope.tables = resp.data;
        });
    };

    $scope.getTable = function(table) {
      var url = ['/api', $scope.db.name, table].join("/");
      $http.get(url)
        .then(function (resp) {
          $scope.table.name = table;
          $scope.table.cols = resp.data;
          console.log(resp.data);
        });
    };

    $scope.createTable = function() {
    };

    $scope.deleteTable = function() {
    };

  }

})();
