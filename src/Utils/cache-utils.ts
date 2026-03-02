import NodeCache from '@cacheable/node-cache'

const caches = {
  lidCache: new NodeCache({ stdTTL: 3600, useClones: false })
  
};
export default caches;
