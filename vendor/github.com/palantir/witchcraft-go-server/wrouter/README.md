wrouter
=======
`wrouter` provides an abstraction for an opinionated HTTP router that defines contracts that make it effective for usage 
with `witchcraft-logging-go` loggers. It also defines an adapter interface that must be implemented and supplied to
actually perform the routing.

Purpose
-------
`wrouter.Router` exists as a common interface for registering routes with path parameters of a certain form and exposing 
a common way to access the path parameters and provide consistent pre and post-request hooks with relevant information.

This allows application/server-level code to perform route registration against the single defined interface in
`wrouter` while still providing flexibility to switch out the actual implementation of the router itself.

Overview
--------
The `wrouter.Router` interface defines a router on which routes can be registered. Routes are registered using the 
following function:

```
Register(method string, path PathTemplate, handler http.Handler, perms RequestParamPerms) error
```

This function registers the provided handler to deal with requests with the provided method that match the specified
path template and uses the specified permissions to determine whether or not request parameters are safe, unsafe or
forbidden.

`PathTemplate` is the template that defines the paths that should be handled by the provided handler. Details on the
acceptable format for path templates are detailed in the "PathTemplate" section.

The `wrouter.Router` can be configured with a `RouteHandler` provider. When this is done, whenever a registered endpoint
is handled, a `RouteHandler` is created and information about the endpoint being invoked is provided to the handler
before and after the request is executed.

The `wrouter.Router` interface defines a contract for how endpoints are registered and provides hooks for handling the
request, but it does not actually perform any routing itself. The `wrouter.RouterImpl` interface defines the interface
that must be satisfied to implement the actual routing, and an instance of this interface must be provided to create a
`wrouter.Router`.

PathTemplate
------------
`PathTemplate` specifies a pattern that is used to match endpoints. A `PathTemplate` can be specified as a string that
matches the following rules:

* Must start with a '/'
* Composed of one or more parts that are separated by a slash ('/')
* A "part" may be one of 3 things:
  * Literal: one or more "plain characters" (defined as alphanumeric characters, '-', and '_')
  * Path param: one or more "plain characters" within a single open and close curly brace
  * Trailing path param: if the part is the final part, it may be one or more "plain characters" followed by a single 
    '*' within a single open and close curly brace
    
For path params and trailing path params, the "plain characters" within the curly braces define the name of the path
parameter. All path parameter names in a given template must be unique (ignoring case).
    
Parts match request paths in the following manner:

* Literal segments match segments IFF all characters in the segment match
* Path param segments match any non-separator value in its segment
* Trailing path param segments match all characters in the final segment (including any separators)

For example, consider the following template:

`/product/{productId}/filePath/{filePath*}`

This template would match the request path `/product/foo123/filePath/var/dir/file.txt`, with path param values
`productId="foo123"` and `filePath="var/dir/file.txt"`. 
