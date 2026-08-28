apiVersion: batch/v1
kind: Job
metadata:
  name: ngkg-release-disruption-qualification
  namespace: ngkg-release-qualification
  labels: {app.kubernetes.io/name: ngkg-release-disruption-qualification, kueue.x-k8s.io/queue-name: ngkg-release-disruption}
spec:
  completionMode: Indexed
  completions: ${NGKG_DISRUPTION_PARTITIONS}
  parallelism: 1
  backoffLimitPerIndex: 0
  maxFailedIndexes: 0
  template:
    metadata: {labels: {app.kubernetes.io/name: ngkg-release-disruption-qualification}}
    spec:
      restartPolicy: Never
      serviceAccountName: ngkg-release-disruption-qualification
      priorityClassName: ngkg-qualification
      containers:
      - name: disruption
        image: ${NGKG_QUALIFICATION_IMAGE_REPOSITORY}@sha256:${NGKG_QUALIFICATION_IMAGE_SHA256}
        args: [scripts/run_phase40_13_24_partition.py, --plan=/evidence/disruption-plan.json, --catalog=/evidence/catalog.json, --partition=$(JOB_COMPLETION_INDEX), --worker-id=$(POD_UID), --driver=/opt/c8/bin/ngkg-release-driver, --allow-disruptive, --approval-evidence=/approval/approval.json, --output=/reports/disruption-$(JOB_COMPLETION_INDEX).json]
        env:
        - name: JOB_COMPLETION_INDEX
          valueFrom: {fieldRef: {fieldPath: "metadata.annotations['batch.kubernetes.io/job-completion-index']"}}
        - name: POD_UID
          valueFrom: {fieldRef: {fieldPath: metadata.uid}}
        - {name: NGKG_RELEASE_WORKER_THREADS, value: '4'}
        resources:
          requests: {cpu: '4', memory: 16Gi}
          limits: {cpu: '4', memory: 16Gi}
        securityContext: {runAsNonRoot: true, allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}, readOnlyRootFilesystem: true, seccompProfile: {type: RuntimeDefault}}
        volumeMounts:
        - {name: evidence, mountPath: /evidence, readOnly: true}
        - {name: approval, mountPath: /approval, readOnly: true}
        - {name: reports, mountPath: /reports}
      volumes:
      - name: evidence
        projected: {sources: [{configMap: {name: ngkg-release-disruption-plan}}, {secret: {name: ngkg-release-inputs}}]}
      - name: approval
        secret: {secretName: ngkg-release-disruption-approval}
      - name: reports
        persistentVolumeClaim: {claimName: ngkg-release-reports}
