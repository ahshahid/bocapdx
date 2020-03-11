app.directive('dragMe', dragMe)
  app.directive('dropOnMe', dropOnMe);

dragMe.$inject = [];

function dragMe() {
  var DDO1 = {
    restrict: 'A',
    link: function(scope, element, attrs) {
      element.prop('draggable', true);
      element.on('dragstart', function(event) {
        /* scope.someCtrlFn(attrs); */
        event.dataTransfer.setData('text', event.target.id)
      });
    }
  };
  return DDO1;
}
dropOnMe.$inject = [];
function dropOnMe() {
  var DDO = {
    restrict: 'A',
    scope: { someCtrlFn: '&callbackFn' },
    link: function(scope, element, attrs) {
      element.on('dragover', function(event) {
        event.preventDefault();
      });
      element.on('drop', function(event) {
        var data = event.dataTransfer.getData("text");
         scope.someCtrlFn({table: data});
        event.preventDefault();
        
        //event.target.appendChild(document.getElementById(data));
      });
    }
  };
  return DDO;
}
app.directive('ngDraggable', function($document) {
  return {
    restrict: 'A',
    scope: {
      dragOptions: '=ngDraggable'
    },
    link: function(scope, elem, attr) {
      var startX, startY, x = 0, y = 0,
          start, stop, drag, container;

      var width  = elem[0].offsetWidth,
          height = elem[0].offsetHeight;

      // Obtain drag options
      if (scope.dragOptions) {
        start  = scope.dragOptions.start;
        drag   = scope.dragOptions.drag;
        stop   = scope.dragOptions.stop;
        var id = scope.dragOptions.container;
        if (id) {
            container = document.getElementById(id).getBoundingClientRect();
            container.x=0;
            container.y=0;
            container.bottom = container.bottom + 15;
            container.height = container.height + 15;
        }
        /* if (id) {
          container = {
            x: 220.828125,
            y: 102,
            width: 1127.15625,
            height: 260,
            top: -10,
            right: 1200,
            bottom: 260,
            left: -10,
          }
      } */
        
      }

      // Bind mousedown event
      elem.on('mousedown', function(e) {
        e.preventDefault();
        startX = e.clientX - elem[0].offsetLeft;
        startY = e.clientY - elem[0].offsetTop;
        $document.on('mousemove', mousemove);
        $document.on('mouseup', mouseup);
        if (start) start(e);
      });

      // Handle drag event
      function mousemove(e) {
        y = e.clientY - startY;
        x = e.clientX - startX;
        setPosition();
        if (drag) drag(e);
      }

      // Unbind drag events
      function mouseup(e) {
        $document.unbind('mousemove', mousemove);
        $document.unbind('mouseup', mouseup);
        if (stop) stop(e);
      }

      // Move element, within container if provided
      function setPosition() {
        if (container) {
          container.left = -10;
          container.top = -10;
          container.bottom = 270;
          container.right = -10;
          if (x < container.left) {
            x = container.left;
          } else if (x > container.right - width) {
            x = container.right - width;
          }
          if (y < container.top) {
            y = container.top;
          } else if (y > container.bottom - height) {
            y = container.bottom - height;
          }
        }

        elem.css({
          top: y + 'px',
          left:  x + 'px'
        });
      }
    }
  }

})