/******************************************************************************/
/*                                                                            */
/*            H A N D L E   M Q   B A C K O U T   M E S S A G E S             */
/*                                                                            */
/*                    M E S S A G E   M A N A G E M E N T                     */
/*                                                                            */
/*                              M S G M N G . C                               */
/*                                                                            */
/* -------------------------------------------------------------------------- */
/*                                                                            */
/*    - initMsgList                                                           */
/*    - initMsgId                                                             */
/*    - chkMsgId                                                              */
/*                                                                            */
/******************************************************************************/

/******************************************************************************/

/******************************************************************************/
/*   I N C L U D E S                                                          */
/******************************************************************************/

// ---------------------------------------------------------
// system
// ---------------------------------------------------------
#include <errno.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

// ---------------------------------------------------------
// MQ
// ---------------------------------------------------------
#include <cmqc.h>

// ---------------------------------------------------------
// own 
// ---------------------------------------------------------
#include "ctl.h"
#include "msgcat/lgstd.h"
#include "msgmng.h"

/******************************************************************************/
/*   T Y P E S                                                                */
/******************************************************************************/
typedef struct sMsgId tMsgId ;

/******************************************************************************/
/*   S T R U C T S                                                            */
/******************************************************************************/
struct sMsgId
{
  MQBYTE24 msgId ;
  time_t   ts    ;
};

/******************************************************************************/
/*   D E F I N E S                                                            */
/******************************************************************************/
#define MAX_MSG_ID_LIST_LNG 1024

/******************************************************************************/
/*   M A C R O S                                                              */
/******************************************************************************/

/******************************************************************************/
/*   G L O B A L S                                                            */
/******************************************************************************/
tMsgId **_gMsgIdList = NULL ;

/******************************************************************************/
/*   P R O T O T Y P E S                                                      */
/******************************************************************************/
tMsgId* initMsgId();

/******************************************************************************/
/*                                                                            */
/*   F U N C T I O N S                                                        */
/*                                                                            */
/******************************************************************************/

/******************************************************************************/
/*   initialize  message id list                                              */
/******************************************************************************/
int initMsgIdList()
{
  logFuncCall() ;               

  int sysRc = 0 ;

  int i;

  _gMsgIdList = (tMsgId**) malloc( MAX_MSG_ID_LIST_LNG *sizeof(tMsgId*));
  if( !_gMsgIdList )
  {
    logger( LSTD_MEM_ALLOC_ERROR );       //
    sysRc = errno ;                       //
    goto _door;                           //
  }

  for( i=0; i<MAX_MSG_ID_LIST_LNG; i++ )
  {
    _gMsgIdList[i] = initMsgId();
    if( !_gMsgIdList[i] )
    {
      sysRc =errno ;
      goto _door;
    }
  }

  _door :

  logFuncExit( );
  return sysRc ;
}

/******************************************************************************/
/*   initialize  message id node                                              */
/******************************************************************************/
tMsgId* initMsgId()
{
  logFuncCall() ;               

  tMsgId* node = NULL ;

  node = (tMsgId*) malloc( sizeof(tMsgId) );

  if( !node )
  {
    logger( LSTD_MEM_ALLOC_ERROR );       //
    goto _door;                           //
  }

  memcpy( node->msgId, MQMI_NONE, sizeof(MQBYTE24) );             
  node->ts = 0 ;

  _door :

  logFuncExit( );
  return node ;
}

/******************************************************************************/
/*   check message id                                                         */
/*                                                                            */
/*    function: chkMsgId                                                      */
/*                                                                            */
/*    description:                                                            */
/*      check if message with message is on the list:                         */
/*      if not found put it on the list                                       */
/*      if found remove it from the list                                      */
/*      if the message can not be put on the list because the list is full,   */
/*      just remove the oldest message from the list                          */
/*      remove all message id's if older then _expiry                         */
/*                                                                            */
/*    return code:                                                            */
/*      MSG_ID_NOT_FOUND:                                                     */
/*          message id not found in the list, message id added to the list    */
/*      MSG_ID_FOUND:                                                         */
/*          message already exists in the list, remove message from the list  */
/*      MSG_ID_OVER_LIST_BUFFER:                                              */
/*          message list is full, message id could not be put on the list     */
/*          message should be put back to the source (original) queue         */
/*          and the oldest message id will be removed from the list           */
/*          because removing the oldest message from the list should not      */
/*          often an extra sleep should be done in the calling function       */
/*                                                                            */
/******************************************************************************/
int chkMsgId( MQBYTE24 _msgId, int _expiry )
{
  logFuncCall() ;               

  int found = MSG_ID_NOT_FOUND ;
  int i;

  time_t ts = time(NULL);
  time_t oldest = LLONG_MAX ;
  int    oldestIx;

  // -------------------------------------------------------
  // check if message id is already in the list
  // -------------------------------------------------------
  for( i=0; i<MAX_MSG_ID_LIST_LNG; i++ )   // go through complete list
  {                                        //
    if( _gMsgIdList[i]->ts == 0 ) continue;// ignore unused nodes in the list
    if( memcmp( _gMsgIdList[i]->msgId,     // compare message id
                _msgId               ,     //
                sizeof(MQBYTE24) ) == 0 )  //
      {                                    //
      _gMsgIdList[i]->ts = 0;              // mark node free for future usage 
      found = MSG_ID_FOUND;                //   by setting the time  to zero
      goto _door;                          // work done, return from function
    }                                      // clean list will be done at _door
  }                                        //  before leaving the function
                                           //
  // -------------------------------------------------------
  // add message id to the list
  // -------------------------------------------------------
  for( i=0; i<MAX_MSG_ID_LIST_LNG; i++ )   // go through complete list
  {                                        //
    if( _gMsgIdList[i]->ts != 0 ) continue;// ignore used nodes in the list
    found = MSG_ID_NOT_FOUND;              // at this stage after it some free
    memcpy( _gMsgIdList[i]->msgId,         //  node has been found,
            _msgId               ,         // copy message id to the list and
            sizeof(MQBYTE24)    );         // set node used by setting the time
    _gMsgIdList[i]->ts = ts;               //   to the system time
    goto _door;                            // work done, return from function
  }                                        // clean list will be done at _door
                                           //  before leaving the function
  found = MSG_ID_OVER_LIST_BUFFER ;        // whole list is used, remove the 
                                           // oldest message id from the list.
  // -------------------------------------------------------
  // find and remove the oldest message id
  // -------------------------------------------------------
  oldestIx = -1 ;                          //
  for( i=0; i<MAX_MSG_ID_LIST_LNG; i++ )   // go through complete list
  {                                        //
    if( _gMsgIdList[i]->ts == 0 ) continue;// ignore unused nodes in the list
    if( _gMsgIdList[i]->ts < oldest )      // the oldest node has the lowest
    {                                      //  time stamp.
      oldest = _gMsgIdList[i]->ts;         //
      oldestIx=i;                          // index of the oldest message id
    }                                      //
  }                                        //
                                           //
  if( oldestIx > -1 )                      // oldest index found, should always 
  {                                        //  be the case since the list 
    _gMsgIdList[oldestIx] = 0 ;            //  is full
  }                                        //
                                           //
  _door:

  // -------------------------------------------------------
  // clean expired message id's
  // -------------------------------------------------------
  for( i=0; i<MAX_MSG_ID_LIST_LNG; i++ )   // messages will be put back to the
  {                                        // original queue, if they can be 
    if( _gMsgIdList[i]->ts == 0 ) continue;// processed in the second loop, than
    if( _gMsgIdList[i]->ts < (ts-_expiry) )// the message id will never be 
    {                                      // removed from the list. So they
      _gMsgIdList[i]->ts = 0;              // have to be removed by time out
    }                                      //
  }                                        //
                                           //
  logFuncExit( );
  return found ;
}
