# Mannequin

This is a dummy HTTP server intended to be reused across multiple synthetic benchmarks.

To build using S2I in Openshift run

```bash
oc process -f build.yaml -p NAMESPACE=my-namespace | oc apply -f -
oc start-build mannequin-builder
oc start-build mannequin
oc create deployment mannequin --image=image-registry.openshift-image-registry.svc:5000/istio-scale/mannequin:latest
```