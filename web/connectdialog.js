define( ['qvangular',
		'text!QlikGoogleCloudConnector.webroot/connectdialog.ng.html',
		'css!QlikGoogleCloudConnector.webroot/connectdialog.css'
], function ( qvangular, template) {
	return {
		template: template,
		controller: ['$scope', 'input', function ( $scope, input ) {
			function init() {
				$scope.isEdit = input.editMode;
				$scope.id = input.instanceId;
				$scope.titleText = $scope.isEdit ? "Change Google Cloud Storage connection" : "Add Google Cloud Storage connection";
				$scope.saveButtonText = $scope.isEdit ? "Save changes" : "Create";

				$scope.path = "";
				$scope.name = "";
				$scope.username = "";
				$scope.password = "";
				$scope.provider = "QlikGoogleCloudConnector.exe"; // Connector filename
				$scope.connectionInfo = "";
				$scope.connectionSuccessful = false;
				$scope.connectionString = createCustomConnectionString($scope.provider, $scope.path);

				input.serverside.sendJsonRequest( "getInfo" ).then( function ( info ) {
					$scope.info = info.qMessage;
				} );

				if ( $scope.isEdit ) {
					input.serverside.getConnection( $scope.id ).then( function ( result ) {
						let cString = result.qConnection.qConnectionString.split('jsonPath=')[1]
						cString = cString.replace(';', '').replace('"', '')

						$scope.name = result.qConnection.qName;
						$scope.path = cString;
					} );
				}
			}


			/* Event handlers */

			$scope.onOKClicked = function () {
				var connString = createCustomConnectionString($scope.provider, $scope.path);
				console.log(connString)

				if ( $scope.isEdit ) {	
					var overrideCredentials = ( $scope.username !== "" && $scope.password !== "" );
					input.serverside.modifyConnection( $scope.id,
						$scope.name,
						connString ,
						$scope.provider,
						overrideCredentials,
						$scope.username,
						$scope.password ).then( function ( result ) {
							if ( result ) {
								$scope.destroyComponent();
							}
						} );
				} else {
					input.serverside.createNewConnection( $scope.name, connString , $scope.username, $scope.password);
					$scope.destroyComponent();
				}
			};

			$scope.onTestConnectionClicked = function () {
				input.serverside.sendJsonRequest( "testConnection", $scope.path).then( function ( info ) {
					$scope.connectionInfo = info.qMessage;
					$scope.connectionSuccessful = info.qMessage.indexOf( "successfully" ) !== -1;
				} );
			};

			$scope.isOkEnabled = function () {
				return $scope.path.length > 0 && $scope.connectionSuccessful;
			};

			$scope.onEscape = $scope.onCancelClicked = function () {
				$scope.destroyComponent();
			};

			
			/* Helper functions */

			function createCustomConnectionString ( provider, connectionstring ) {
				return "CUSTOM CONNECT TO " + "\"provider=" + provider + ";jsonPath=" + connectionstring + ";" + "\"";
			}


			init();
		}]
	};
} );