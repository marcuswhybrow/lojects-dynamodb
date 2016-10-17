function _flatten(obj, prefix) {
  return Object.keys(obj).map(key => {
    const val = obj[key];
    if (isObject(val)) {
      return _flatten(val, `${prefix}.${key}`)
    }
    let ret = {};
    ret[`${prefix}.${key}`] = val;
    return ret;
  }).reduce((obj, currentArray) => {
    Object.keys(currentArray).forEach(key => {
      obj[key] = currentArray[key];
    });
    return obj;
  }, {});
}

function flatten(obj) {
  let results = _flatten(obj, '');
  Object.keys(results).forEach(key => {
    results[key.substring(1)] = results[key];
    delete results[key];
  });
  return results;
}
